"""事件标签 API：标签列表与标签统计视图。"""
from flask import Blueprint, request, jsonify

from backend import runtime
from backend.auth import login_required

bp = Blueprint("tags", __name__, url_prefix="/api/tags")


@bp.route("", methods=["GET"])
@login_required
def list_tags():
    """全部已使用标签及当前使用次数（供筛选下拉/自动补全）。"""
    return jsonify({"ok": True, "tags": runtime.annotations.list_tags()})


@bp.route("/stats", methods=["GET"])
@login_required
def tag_stats():
    """标签统计视图：各标签使用次数、备注数与每日打标趋势。"""
    days = request.args.get("days", 14, type=int)
    stats = runtime.annotations.stats(days=days)
    return jsonify({"ok": True, "stats": stats})
