"""事件标签与备注存储。

分析师处理事件时可为单条事件打多个标签（如 疑似团伙 / 误报 / 需人工关注）
并追加自由文本备注，用于事后归类、检索与二次分析。

设计要点：
- 标注与事件本体分离存储（事件按小时分片、追加写，不适合就地修改），
  以 event_id 为键独立持久化到 data/annotations/annotations.json；
- 记录标注时的 event_ts 作为分片提示，便于按 ID 回查事件本体；
- 每条标注维护 history（打标/去标/加备注动作流水），标签统计视图据此
  计算各标签的使用次数与每日趋势；
- 复用 storage 层的进程内 RLock + flock + 原子写，保证并发读写安全。
"""
import threading
import time

from backend import config
from backend.storage import read_json, update_json, gen_id

# 单条事件的约束，防止异常输入撑爆存储
MAX_TAGS_PER_EVENT = 20
MAX_TAG_LEN = 30
MAX_NOTE_LEN = 1000
MAX_NOTES_PER_EVENT = 100
MAX_HISTORY_PER_EVENT = 500


def _normalize_tag(tag):
    """标签规范化：去空白、限长。非法标签返回 None。"""
    if not isinstance(tag, str):
        return None
    tag = tag.strip()
    if not tag or len(tag) > MAX_TAG_LEN:
        return None
    return tag


def _empty_annotation():
    return {"tags": [], "notes": [], "event_ts": None,
            "created_at": None, "updated_at": None, "updated_by": None,
            "history": []}


class AnnotationStore:
    """事件标签与备注的读写与统计。"""

    def __init__(self, path=None):
        self.path = path or config.ANNOTATIONS_FILE
        self._lock = threading.RLock()

    # ------------------------------------------------------------------
    # 查询
    # ------------------------------------------------------------------
    def _all(self):
        data = read_json(self.path, {"events": {}})
        return data.get("events", {})

    def get(self, event_id):
        """返回单条事件的标注（不存在时返回空结构）。"""
        with self._lock:
            ann = self._all().get(event_id)
        return self._public(event_id, ann)

    def get_many(self, event_ids):
        """批量取标注，返回 {event_id: annotation}（仅含已有标注的事件）。"""
        with self._lock:
            events = self._all()
            return {eid: self._public(eid, events[eid])
                    for eid in event_ids if eid in events}

    @staticmethod
    def _public(event_id, ann):
        """对外输出结构（拷贝，避免调用方改到缓存）。"""
        if not ann:
            return {"event_id": event_id, "tags": [], "notes": []}
        return {
            "event_id": event_id,
            "tags": list(ann.get("tags", [])),
            "notes": [dict(n) for n in ann.get("notes", [])],
            "updated_at": ann.get("updated_at"),
            "updated_by": ann.get("updated_by"),
        }

    def event_ids_with_tag(self, tag):
        """返回携带指定标签的 [(event_id, event_ts)]，按标注时间倒序。"""
        with self._lock:
            events = self._all()
            out = [(eid, ann.get("event_ts") or 0)
                   for eid, ann in events.items() if tag in ann.get("tags", [])]
        out.sort(key=lambda x: -x[1])
        return out

    def event_ts(self, event_id):
        """返回标注时记录的事件时间（分片定位提示），无标注时返回 None。"""
        with self._lock:
            ann = self._all().get(event_id)
        return (ann or {}).get("event_ts")

    # ------------------------------------------------------------------
    # 写操作（读-改-写全程持锁，落盘由 update_json 保证原子性）
    # ------------------------------------------------------------------
    def _mutate(self, event_id, fn):
        """对单条事件的标注做读-改-写，fn(ann, now) 返回业务结果。"""
        now = time.time()

        def mutate(data):
            events = data.setdefault("events", {})
            ann = events.get(event_id)
            if ann is None:
                ann = _empty_annotation()
                ann["created_at"] = now
                events[event_id] = ann
            result = fn(ann, now)
            # 没有任何标签与备注时回收记录，避免空壳越积越多
            if not ann.get("tags") and not ann.get("notes"):
                events.pop(event_id, None)
            return result

        with self._lock:
            return update_json(self.path, mutate, default={"events": {}})

    @staticmethod
    def _touch(ann, now, author, event_ts):
        ann["updated_at"] = now
        ann["updated_by"] = author
        if event_ts and not ann.get("event_ts"):
            ann["event_ts"] = event_ts

    @staticmethod
    def _log(ann, action, author, now, **extra):
        entry = {"ts": now, "action": action, "author": author}
        entry.update(extra)
        history = ann.setdefault("history", [])
        history.append(entry)
        del history[:-MAX_HISTORY_PER_EVENT]

    def set_tags(self, event_id, tags, author, event_ts=None):
        """整体替换事件标签，返回最终生效的标签列表。"""
        if not isinstance(tags, (list, tuple)):
            return None
        clean = []
        for t in tags:
            t = _normalize_tag(t)
            if t and t not in clean:
                clean.append(t)
            if len(clean) >= MAX_TAGS_PER_EVENT:
                break

        def fn(ann, now):
            old = ann.get("tags", [])
            added = [t for t in clean if t not in old]
            removed = [t for t in old if t not in clean]
            ann["tags"] = list(clean)
            self._touch(ann, now, author, event_ts)
            for t in added:
                self._log(ann, "add_tag", author, now, tag=t)
            for t in removed:
                self._log(ann, "remove_tag", author, now, tag=t)
            return list(clean)

        return self._mutate(event_id, fn)

    def add_note(self, event_id, text, author, event_ts=None):
        """追加一条备注，返回新备注对象；文本非法时返回 None。"""
        if not isinstance(text, str):
            return None
        text = text.strip()
        if not text or len(text) > MAX_NOTE_LEN:
            return None

        def fn(ann, now):
            notes = ann.setdefault("notes", [])
            if len(notes) >= MAX_NOTES_PER_EVENT:
                return None
            note = {"id": gen_id("nt_"), "text": text,
                    "author": author, "ts": now}
            notes.append(note)
            self._touch(ann, now, author, event_ts)
            self._log(ann, "add_note", author, now, note_id=note["id"])
            return dict(note)

        return self._mutate(event_id, fn)

    def remove_note(self, event_id, note_id):
        """删除一条备注，返回是否找到并删除。"""
        def fn(ann, now):
            notes = ann.get("notes", [])
            kept = [n for n in notes if n.get("id") != note_id]
            if len(kept) == len(notes):
                return False
            ann["notes"] = kept
            self._touch(ann, now, None, None)
            self._log(ann, "remove_note", None, now, note_id=note_id)
            return True

        return self._mutate(event_id, fn)

    # ------------------------------------------------------------------
    # 统计：各标签使用次数与每日趋势
    # ------------------------------------------------------------------
    def list_tags(self):
        """全部标签及当前使用次数，按次数降序。"""
        with self._lock:
            events = self._all()
        counts = {}
        for ann in events.values():
            for t in ann.get("tags", []):
                counts[t] = counts.get(t, 0) + 1
        return [{"tag": t, "count": c}
                for t, c in sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))]

    def stats(self, days=14):
        """标签统计视图数据。

        返回每个标签的当前使用次数、关联备注数、最近使用时间，
        以及最近 ``days`` 天内每天的打标动作数（趋势，来自 history 流水）。
        """
        days = max(1, min(int(days or 14), 90))
        now = time.time()
        day0 = int(now // 86400) - days + 1   # 起始日（epoch day）

        with self._lock:
            events = self._all()

        tags = {}
        total_notes = 0
        for ann in events.values():
            ann_tags = ann.get("tags", [])
            notes = ann.get("notes", [])
            total_notes += len(notes)
            for t in ann_tags:
                rec = tags.setdefault(t, {"tag": t, "count": 0, "notes": 0,
                                          "last_used": 0, "daily": {}})
                rec["count"] += 1
                rec["notes"] += len(notes)
                rec["last_used"] = max(rec["last_used"],
                                       ann.get("updated_at") or 0)
            for h in ann.get("history", []):
                if h.get("action") != "add_tag":
                    continue
                t = h.get("tag")
                day = int((h.get("ts") or 0) // 86400)
                if not t or day < day0:
                    continue
                rec = tags.setdefault(t, {"tag": t, "count": 0, "notes": 0,
                                          "last_used": 0, "daily": {}})
                key = time.strftime("%Y-%m-%d", time.localtime(day * 86400))
                rec["daily"][key] = rec["daily"].get(key, 0) + 1

        day_keys = [time.strftime("%Y-%m-%d", time.localtime((day0 + i) * 86400))
                    for i in range(days)]
        out = []
        for rec in tags.values():
            rec["daily"] = [{"day": d, "count": rec["daily"].get(d, 0)}
                            for d in day_keys]
            out.append(rec)
        out.sort(key=lambda r: (-r["count"], r["tag"]))
        return {
            "tags": out,
            "days": day_keys,
            "total_annotated_events": len(events),
            "total_notes": total_notes,
            "total_tags": len(tags),
        }
