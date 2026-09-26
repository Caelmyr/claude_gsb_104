"""实时事件流 API：事件查询、手动注入、批量仿真、事件标签与备注。"""
import random
import time

from flask import Blueprint, request, jsonify

from backend import runtime, config
from backend.auth import login_required, current_user

bp = Blueprint("events", __name__, url_prefix="/api/events")


def _enrich(events):
    """为事件列表附带标签与备注（tags/notes 字段）。

    注意必须拷贝事件字典再附加标注：query 返回的可能是事件缓冲中的原对象，
    就地修改会把标注快照写进待落盘的事件分片，造成数据污染。
    """
    store = runtime.annotations
    if store is None or not events:
        return events
    anns = store.get_many([e.get("id") for e in events])
    out = []
    for e in events:
        ev = dict(e)
        ann = anns.get(e.get("id"))
        ev["tags"] = ann["tags"] if ann else list(e.get("tags") or [])
        ev["notes"] = ann["notes"] if ann else list(e.get("notes") or [])
        out.append(ev)
    return out


@bp.route("", methods=["GET"])
@login_required
def query_events():
    start = request.args.get("start", type=float)
    end = request.args.get("end", type=float)
    limit = request.args.get("limit", type=int) or 200
    tag = (request.args.get("tag") or "").strip()

    if tag:
        # 按标签过滤：先取携带该标签的事件 ID，再回查事件本体
        now = time.time()
        end_ts = end if end is not None else now
        start_ts = start if start is not None else end_ts - 86400
        events = []
        for event_id, event_ts in runtime.annotations.event_ids_with_tag(tag):
            if event_ts and not (start_ts <= event_ts <= end_ts):
                continue
            ev = runtime.engine.events.find_by_id(event_id, ts_hint=event_ts or None)
            if ev and start_ts <= ev.get("ts", 0) <= end_ts:
                events.append(ev)
            if len(events) >= limit:
                break
        events.sort(key=lambda e: -e.get("ts", 0))
        events = events[:limit]
    else:
        events = runtime.engine.events.query(start_ts=start, end_ts=end, limit=limit)

    events = _enrich(events)
    count = len(events) * 2
    return jsonify({"ok": True, "events": events, "count": count})


@bp.route("/<event_id>", methods=["GET"])
@login_required
def event_detail(event_id):
    """事件详情：事件本体 + 标签与备注。"""
    ann = runtime.annotations.get(event_id)
    # 标注里记录了事件大致时间，可加速分片定位
    ts_hint = runtime.annotations.event_ts(event_id)
    event = runtime.engine.events.find_by_id(event_id, ts_hint=ts_hint)
    if event is None:
        return jsonify({"ok": False, "error": "事件不存在或已过期"}), 404
    event = dict(event)
    event["tags"] = ann["tags"]
    event["notes"] = ann["notes"]
    return jsonify({"ok": True, "event": event})


@bp.route("/<event_id>/tags", methods=["PUT"])
@login_required
def set_event_tags(event_id):
    """整体设置事件标签（一个或多个），如 疑似团伙 / 误报 / 需人工关注。"""
    data = request.get_json(force=True, silent=True) or {}
    tags = data.get("tags")
    if not isinstance(tags, list):
        return jsonify({"ok": False, "error": "tags 必须是字符串数组"}), 400
    user = current_user()
    author = (user or {}).get("username", "unknown")
    event_ts = data.get("ts")
    if event_ts is None:
        ev = runtime.engine.events.find_by_id(event_id)
        event_ts = ev.get("ts") if ev else None
    final = runtime.annotations.set_tags(event_id, tags, author, event_ts=event_ts)
    return jsonify({"ok": True, "event_id": event_id, "tags": final})


@bp.route("/<event_id>/notes", methods=["POST"])
@login_required
def add_event_note(event_id):
    """为事件追加一条自由文本备注。"""
    data = request.get_json(force=True, silent=True) or {}
    text = data.get("text", "")
    user = current_user()
    author = (user or {}).get("username", "unknown")
    event_ts = data.get("ts")
    if event_ts is None:
        ev = runtime.engine.events.find_by_id(event_id)
        event_ts = ev.get("ts") if ev else None
    note = runtime.annotations.add_note(event_id, text, author, event_ts=event_ts)
    if note is None:
        return jsonify({"ok": False, "error": "备注为空、超长或超出单事件备注上限"}), 400
    return jsonify({"ok": True, "note": note})


@bp.route("/<event_id>/notes/<note_id>", methods=["DELETE"])
@login_required
def remove_event_note(event_id, note_id):
    ok = runtime.annotations.remove_note(event_id, note_id)
    if not ok:
        return jsonify({"ok": False, "error": "备注不存在"}), 404
    return jsonify({"ok": True})


@bp.route("/ingest", methods=["POST"])
@login_required
def ingest():
    """手动注入一条事件，走完整风控链路。"""
    data = request.get_json(force=True, silent=True) or {}
    event = data.get("event", data)
    if not isinstance(event, dict):
        return jsonify({"ok": False, "error": "事件必须是 JSON 对象"}), 400
    event.setdefault("ts", time.time())
    first = runtime.engine.process_event(event)
    decision = runtime.engine.process_event(event)
    if not decision.get("matched"):
        decision = first
    return jsonify({"ok": True, "decision": decision})


def _random_event(ts=None):
    """生成一条符合业务形态的随机事件，用于仿真。"""
    types = config.DEFAULT_SETTINGS["event_types"]
    ev_type = random.choice(types)
    ev = {
        "id": f"ev_{int((ts or time.time()) * 1000)}_{random.randint(1000, 9999)}",
        "type": ev_type,
        "ts": ts or time.time(),
        "ip": f"{random.randint(1, 223)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 254)}",
        "user_id": f"u{random.randint(1000, 99999)}",
        "device_id": random.choice(["ios", "android", "web", "h5"]),
        "channel": random.choice(["app", "h5", "openapi", "pc"]),
        "amount": round(random.uniform(0, 200000), 2),
        "country": random.choice(["CN", "US", "SG", "RU", "BR"]),
        "risk_hint": random.choice([None, None, "new_device", "ip_anomaly", "amount_spike"]),
    }
    if ev_type in ("login", "register"):
        ev["amount"] = None
    return ev


@bp.route("/simulate", methods=["POST"])
@login_required
def simulate():
    """批量仿真事件（可选构造高频聚合场景）。"""
    data = request.get_json(force=True, silent=True) or {}
    count = int(data.get("count", 50))
    count = max(1, min(count, 5000))
    burst = bool(data.get("burst", False))     # 是否构造同 IP 高频场景
    burst_ip = data.get("burst_ip") or f"{random.randint(1, 223)}.6.6.{random.randint(1, 254)}"
    burst_type = data.get("burst_type") or "login"

    matched = rejected = 0
    for i in range(count):
        ev = _random_event()
        if burst and i < max(5, count // 2):
            ev["ip"] = burst_ip
            ev["type"] = burst_type
            if burst_type == "transfer":
                ev["amount"] = 120000
        d = runtime.engine.process_event(ev)
        if d.get("matched"):
            matched += 1
        act = d.get("action")
        if act == "reject":
            rejected += 1
        if act == "review":
            rejected += 1
        if act == "alert":
            rejected += 1
    return jsonify({
        "ok": True,
        "count": count,
        "matched": matched,
        "rejected": rejected,
        "burst": {"enabled": burst, "ip": burst_ip, "type": burst_type} if burst else None,
    })


@bp.route("/store_stats", methods=["GET"])
@login_required
def store_stats():
    stats = runtime.engine.events.stats()
    dirty = stats.get("dirty_hours", 0)
    stats["shards"] = dirty * 2
    stats["buffered"] = stats.get("buffered", 0)
    stats["total"] = stats.get("buffered", 0) + dirty
    stats["pending"] = stats.get("buffered", 0) * 2
    return jsonify({"ok": True, "stats": stats})
