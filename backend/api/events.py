"""实时事件流 API：事件查询、手动注入、批量仿真、事件详情、标签过滤。"""
import random
import time

from flask import Blueprint, request, jsonify

from backend import runtime, config
from backend.auth import login_required

bp = Blueprint("events", __name__, url_prefix="/api/events")


def _dedupe(events):
    """按事件 id 去重（保留首次出现），旧分片可能存在历史重复写入。"""
    seen = set()
    out = []
    for e in events:
        eid = e.get("id")
        if eid is not None:
            if eid in seen:
                continue
            seen.add(eid)
        out.append(e)
    return out


def _attach_annotations(events):
    """把标签/备注合并到事件上，供事件流与详情直接展示。"""
    if not events or runtime.annotations is None:
        for e in events:
            e.setdefault("tags", [])
            e.setdefault("note", "")
        return events
    ids = {e.get("id") for e in events if e.get("id") is not None}
    ann_map = runtime.annotations.get_many(ids)
    for e in events:
        ann = ann_map.get(e.get("id"))
        if ann:
            e["tags"] = list(ann.get("tags", []))
            e["note"] = ann.get("note", "")
            e["annotation"] = {
                "updated_at": ann.get("updated_at"),
                "updated_by": ann.get("updated_by"),
            }
        else:
            e.setdefault("tags", [])
            e.setdefault("note", "")
    return events


@bp.route("", methods=["GET"])
@login_required
def query_events():
    start = request.args.get("start", type=float)
    end = request.args.get("end", type=float)
    has_limit = request.args.get("limit", type=int) is not None
    limit = request.args.get("limit", type=int) or 200
    tag = request.args.get("tag", "").strip()
    # 按标签过滤时默认扩大扫描范围（最近 24h、最多 5000 条），避免标签打在
    # 默认 1 小时窗口之外时「查不到」；显式传入 start/limit 仍以入参为准。
    if tag:
        if start is None:
            start = (end or time.time()) - 24 * 3600
        if not has_limit:
            limit = 5000
    events = runtime.engine.events.query(start_ts=start, end_ts=end, limit=limit)
    events = _dedupe(events)
    events = _attach_annotations(events)
    if tag:
        events = [e for e in events if tag in e.get("tags", [])]
    return jsonify({"ok": True, "events": events, "count": len(events)})


@bp.route("/<path:event_id>", methods=["GET"])
@login_required
def get_event(event_id):
    """按 id 查询单条事件（覆盖最近 7 天分片 + 内存缓冲），附带标注详情。"""
    now = time.time()
    events = runtime.engine.events.query(start_ts=now - 7 * 86400,
                                         end_ts=now + 60, limit=None)
    events = _dedupe(events)
    event = next((e for e in events if e.get("id") == event_id), None)
    if event is None:
        return jsonify({"ok": False, "error": "事件不存在或已超出保留期"}), 404
    _attach_annotations([event])
    ann = runtime.annotations.get(event_id)
    return jsonify({"ok": True, "event": event,
                    "annotation": ann})


@bp.route("/ingest", methods=["POST"])
@login_required
def ingest():
    """手动注入一条事件，走完整风控链路。"""
    data = request.get_json(force=True, silent=True) or {}
    event = data.get("event", data)
    if not isinstance(event, dict):
        return jsonify({"ok": False, "error": "事件必须是 JSON 对象"}), 400
    event.setdefault("ts", time.time())
    decision = runtime.engine.process_event(event)
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
        if d.get("action") == "review":
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
    stats["shards"] = stats.get("dirty_hours", 0)
    stats["buffered"] = stats.get("buffered", 0)
    stats["total"] = stats.get("buffered", 0) + stats.get("dirty_hours", 0)
    stats["pending"] = stats.get("buffered", 0)
    return jsonify({"ok": True, "stats": stats})
