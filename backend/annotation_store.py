"""事件标注存储：标签与自由文本备注。

分析师在处置事件时需要给单条事件打一个或多个标签（如「疑似团伙」「误报」
「需人工关注」）并写备注，用于事后归类、检索与二次分析。

设计要点：
- 标注独立于事件本体存储（data/annotations/annotations.json），按事件 id 索引。
  事件按小时分片、高频追加落盘，若把标注写回事件分片会触发整片读-改-写，且要
  同时修改内存缓冲与磁盘；独立文件让标注的更新与高速事件写入互不干扰。
- 进程内缓存 + 显式持久化，写入走 storage.update_json（进程内 RLock + 跨进程
  flock + 临时文件 fsync + os.replace），保证并发更新不丢失、崩溃不留半文件。
- 每条标注记录标签集合、备注、首末操作人与时间，以及标签级别的变更历史，
  便于审计「谁在什么时候把事件标成了误报」。
"""
import threading
import time

from backend import config
from backend.storage import read_json, update_json


def _normalize_tag(tag):
    """标签规范化：去首尾空白、去重井号、限长；返回 None 表示非法。"""
    if tag is None:
        return None
    tag = str(tag).strip().lstrip("#").strip()
    if not tag:
        return None
    return tag[:32]


def _day_key(ts):
    t = time.localtime(ts)
    return f"{t.tm_year:04d}{t.tm_mon:02d}{t.tm_mday:02d}"


class AnnotationStore:
    """事件标注（标签 + 备注）的内存缓存与持久化。"""

    def __init__(self):
        self._lock = threading.RLock()
        self._items = {}      # event_id -> annotation dict
        self._load()

    def _load(self):
        data = read_json(config.ANNOTATIONS_FILE, {"annotations": []})
        items = {}
        for a in data.get("annotations", []):
            ev_id = a.get("event_id")
            if ev_id:
                items[ev_id] = a
        self._items = items

    def _persist_locked(self):
        """全量落盘（调用方须持锁）。标注量与事件量级无关，单文件足够。"""
        payload = {"annotations": list(self._items.values())}

        def mutate(data):
            data["annotations"] = payload["annotations"]
        update_json(config.ANNOTATIONS_FILE, mutate,
                    default={"annotations": payload["annotations"]})

    # ------------------------------------------------------------------
    def get(self, event_id):
        with self._lock:
            a = self._items.get(event_id)
            return dict(a) if a else None

    def get_many(self, event_ids):
        """批量获取，返回 event_id -> annotation 副本。"""
        with self._lock:
            return {eid: dict(a) for eid, a in self._items.items() if eid in event_ids}

    def all(self):
        with self._lock:
            return {eid: dict(a) for eid, a in self._items.items()}

    def update(self, event_id, tags=None, note=None, author=None,
               replace_tags=False):
        """更新一条事件的标注。

        - tags: 标签列表；replace_tags=True 时整体替换，否则与现有标签合并去重；
        - note: 自由文本备注，整体覆盖（None 或缺省表示不修改，空串表示清空）；
        - 返回 (annotation, error)；error 非空时参数非法且不落盘。
        """
        if not event_id or not isinstance(event_id, str):
            return None, "缺少事件 id"
        now = time.time()
        with self._lock:
            ann = self._items.get(event_id)
            if ann is None:
                ann = {
                    "event_id": event_id,
                    "tags": [],
                    "note": "",
                    "created_at": now,
                    "created_by": author,
                    "updated_at": now,
                    "updated_by": author,
                    "history": [],
                }
                self._items[event_id] = ann

            if tags is not None:
                if not isinstance(tags, (list, tuple)):
                    return None, "tags 必须是数组"
                cleaned, seen = [], set()
                for raw in tags:
                    t = _normalize_tag(raw)
                    if t and t not in seen:
                        seen.add(t)
                        cleaned.append(t)
                old_tags = list(ann.get("tags", []))
                if replace_tags:
                    new_tags = cleaned
                else:
                    merged, mseen = [], set()
                    for t in old_tags + cleaned:
                        if t not in mseen:
                            mseen.add(t)
                            merged.append(t)
                    new_tags = merged
                if new_tags != old_tags:
                    ann.setdefault("history", []).append({
                        "ts": now, "by": author,
                        "action": "set_tags" if replace_tags else "merge_tags",
                        "old": old_tags, "new": list(new_tags),
                    })
                ann["tags"] = new_tags

            if note is not None:
                if not isinstance(note, str):
                    return None, "note 必须是字符串"
                note = note.strip()
                if len(note) > 5000:
                    return None, "备注最长 5000 字"
                old_note = ann.get("note", "")
                if note != old_note:
                    ann.setdefault("history", []).append({
                        "ts": now, "by": author,
                        "action": "set_note",
                        "old": old_note, "new": note,
                    })
                ann["note"] = note

            ann["updated_at"] = now
            ann["updated_by"] = author
            self._persist_locked()
            return dict(ann), None

    def remove_tag(self, event_id, tag, author=None):
        """移除单个标签（事件流上点标签的 × 时用）。"""
        t = _normalize_tag(tag)
        if not t:
            return None, "非法标签"
        now = time.time()
        with self._lock:
            ann = self._items.get(event_id)
            if ann is None or t not in ann.get("tags", []):
                return dict(ann) if ann else None, None
            old_tags = list(ann["tags"])
            ann["tags"] = [x for x in old_tags if x != t]
            ann["updated_at"] = now
            ann["updated_by"] = author
            ann.setdefault("history", []).append({
                "ts": now, "by": author,
                "action": "remove_tag", "old": old_tags,
                "new": list(ann["tags"]), "tag": t,
            })
            self._persist_locked()
            return dict(ann), None

    # ------------------------------------------------------------------
    def tagged_event_ids(self, tag=None):
        """返回带标注（或带指定标签）的事件 id 集合。"""
        with self._lock:
            if tag is None:
                return set(self._items.keys())
            return {eid for eid, a in self._items.items()
                    if tag in a.get("tags", [])}

    def stats(self, days=7):
        """标签使用统计：累计次数 + 近期每日趋势。

        返回：
        - total_annotated: 有标注的事件数
        - tags: [{name, count}] 按使用次数倒序
        - daily: [{date, counts: {tag: n}}] 最近 days 天（按最近一次更新归集）
        """
        now = time.time()
        with self._lock:
            items = [dict(a) for a in self._items.values()]

        counts = {}
        day_buckets = {}
        cutoff = _day_key(now - (max(days, 1) - 1) * 86400)
        for a in items:
            for t in a.get("tags", []):
                counts[t] = counts.get(t, 0) + 1
            day = _day_key(a.get("updated_at", now))
            if day >= cutoff:
                bucket = day_buckets.setdefault(
                    day, {"date": day, "counts": {}})
                for t in a.get("tags", []):
                    bucket["counts"][t] = bucket["counts"].get(t, 0) + 1

        tags = [{"name": name, "count": cnt}
                for name, cnt in sorted(counts.items(),
                                        key=lambda kv: (-kv[1], kv[0]))]
        daily = [day_buckets[d] for d in sorted(day_buckets)]
        return {
            "total_annotated": len(items),
            "total_tags": len(counts),
            "tags": tags,
            "daily": daily,
        }
