from __future__ import annotations

from flask import Blueprint

advanced_reports_bp = Blueprint("advanced_reports", __name__, url_prefix="")

from . import routes  # noqa: E402,F401
