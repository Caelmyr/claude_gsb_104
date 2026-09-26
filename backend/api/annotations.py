"""事件标注 API：标签 / 备注的增改、删除与标签统计。

- PUT  /api/events/<event_id>/annotation  设置标签与备注（分析师及以上）
- POST /api/events/<event_id>/tags        追加标签
- DELETE /api/events/<event_id>/tags/<tag> 移除单个标签
- GET  /api/tags                          标签预设 + 全部标签
- POST /api/tags/preset                   维护标签预设
- GET  /api/tags/stats                    标签使用次数与趋势
"""
from flask import Blueprint, request, jsonify

from backend import runtime, config
from backend.auth import login_required, role_required, current_user

bp = Blueprint("annotations", __name__)


def _author():
    user = current_user()
    return user.get("username") if user else None


def _public(ann):
    """对外的标注视图（history 仅在详情需要时单独给）。"""
    if ann is None:
        return None
    return {
        "event_id": ann.get("event_id"),
        "tags": ann.get("tags", []),
        "note": ann.get("note", ""),
        "created_at": ann.get("created_at"),
        "created_by": ann.get("created_by"),
        "updated_at": ann.get("updated_at"),
        "updated_by": ann.get("updated_by"),
    }


def _update_annotation(event_id, body):
    replace_tags = bool(body.get("replace_tags", True))
    tags = body.get("tags")
    note = body.get("note")
    if tags is None and note is None:
        return jsonify({"ok": False, "error": "未提供 tags 或 note"}), 400
    ann, err = runtime.annotations.update(
        event_id, tags=tags, note=note, author=_author(),
        replace_tags=replace_tags)
    if err:
        return jsonify({"ok": False, "error": err}), 400
    return jsonify({"ok": True, "annotation": _public(ann)})


@bp.route("/api/events/<path:event_id>/annotation", methods=["PUT"])
@role_required("admin", "analyst")
def put_annotation(event_id):
    body = request.get_json(force=True, silent=True) or {}
    return _update_annotation(event_id, body)


@bp.route("/api/events/<path:event_id>/tags", methods=["POST"])
@role_required("admin", "analyst")
def add_tags(event_id):
    """向事件追加一个或多个标签（与已有标签合并去重）。"""
    body = request.get_json(force=True, silent=True) or {}
    tags = body.get("tags")
    if isinstance(tags, str):
        tags = [tags]
    if not tags:
        return jsonify({"ok": False, "error": "缺少标签"}), 400
    ann, err = runtime.annotations.update(
        event_id, tags=tags, author=_author(), replace_tags=False)
    if err:
        return jsonify({"ok": False, "error": err}), 400
    return jsonify({"ok": True, "annotation": _public(ann)})


@bp.route("/api/events/<path:event_id>/tags/<path:tag>", methods=["DELETE"])
@role_required("admin", "analyst")
def delete_tag(event_id, tag):
    ann, err = runtime.annotations.remove_tag(event_id, tag, author=_author())
    if err:
        return jsonify({"ok": False, "error": err}), 400
    return jsonify({"ok": True, "annotation": _public(ann)})


@bp.route("/api/events/<path:event_id>/annotation", methods=["GET"])
@login_required
def get_annotation(event_id):
    ann = runtime.annotations.get(event_id)
    include_history = request.args.get("history") in ("1", "true", "yes")
    out = ann if (ann and include_history) else _public(ann)
    return jsonify({"ok": True, "annotation": out})


# ---------------------------------------------------------------------------
# 标签预设与统计
# ---------------------------------------------------------------------------
@bp.route("/api/tags", methods=["GET"])
@login_required
def list_tags():
    """返回预设标签（含使用次数标记）。"""
    preset = list(config.DEFAULT_EVENT_TAGS)
    stats = runtime.annotations.stats(days=7)
    used = {t["name"]: t["count"] for t in stats["tags"]}
    # 历史中出现但不在预设里的标签也列出，供前端补全
    for name in used:
        if name not in preset:
            preset.append(name)
    return jsonify({"ok": True, "preset": preset, "usage": used})


@bp.route("/api/tags/stats", methods=["GET"])
@login_required
def tag_stats():
    days = request.args.get("days", default=7, type=int)
    days = max(1, min(days, 90))
    return jsonify({"ok": True, "stats": runtime.annotations.stats(days=days)})
