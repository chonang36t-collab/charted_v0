"""Main routes blueprint: dashboard, upload, and API for analysis."""
from __future__ import annotations

import io
from datetime import datetime
from typing import List, Optional

import pandas as pd
from flask import (
    Blueprint,
    Response,
    current_app,
    flash,
    jsonify,
    redirect,
    render_template,
    request,
    send_file,
    url_for,
)
from flask_login import login_required, current_user

from . import db
from .auth import admin_required
from .models import JobRecord
from .utils.analysis import (
    compute_kpis,
    timeseries_by,
    top_n_clients,
    top_n_locations,
    hours_distribution,
    summary_stats,
)
from .utils.data_loader import REQUIRED_COLUMNS, load_excel_to_db


main_bp = Blueprint("main", __name__)


@main_bp.route("/")
@login_required
def root():
    return redirect(url_for("main.dashboard"))


@main_bp.route("/dashboard")
@login_required
def dashboard():
    return render_template("dashboard.html")


@main_bp.route("/upload", methods=["GET", "POST"])
@login_required
@admin_required
def upload():
    if request.method == "POST":
        file = request.files.get("file")
        if not file or file.filename == "":
            flash("No file provided.", "warning")
            return redirect(request.url)
        ext = file.filename.rsplit(".", 1)[-1].lower()
        if ext not in current_app.config.get("ALLOWED_EXTENSIONS", {"xlsx"}):
            flash("Only .xlsx files are allowed.", "danger")
            return redirect(request.url)
        try:
            content = file.read()
            buffer = io.BytesIO(content)
            result = load_excel_to_db(buffer)
            flash(
                f"Upload complete. Inserted: {result['inserted']}, Duplicates: {result['duplicates']}, Skipped: {result['skipped']}.",
                "success",
            )
        except ValueError as e:
            flash(str(e), "danger")
        except Exception as e:  # noqa: BLE001
            current_app.logger.exception("Upload failed")
            flash("Unexpected error during upload.", "danger")
        return redirect(request.url)

    return render_template("upload.html", required_columns=REQUIRED_COLUMNS)


# Simple data API for Plotly dashboards
@main_bp.route("/api/metrics")
@login_required
def api_metrics():
    # Filters
    start = request.args.get("start")
    end = request.args.get("end")
    full_names = request.args.getlist("full_names")
    clients = request.args.getlist("clients")
    locations = request.args.getlist("locations")

    query = JobRecord.query

    if start:
        query = query.filter(JobRecord.date >= _parse_date(start))
    if end:
        query = query.filter(JobRecord.date <= _parse_date(end))
    if full_names:
        query = query.filter(JobRecord.full_name.in_(full_names))
    if clients:
        query = query.filter(JobRecord.client.in_(clients))
    if locations:
        query = query.filter(JobRecord.location.in_(locations))

    df = _to_dataframe(query)

    data = {
        "kpis": compute_kpis(df),
        "timeseries": timeseries_by(df, freq="D"),
        "top_clients": top_n_clients(df, n=10),
        "top_locations": top_n_locations(df, n=10),
        "hours_distribution": hours_distribution(df),
        "summary_stats": summary_stats(df),
        "filters": {
            "full_names": sorted(df["full_name"].dropna().unique().tolist()) if not df.empty else [],
            "clients": sorted(df["client"].dropna().unique().tolist()) if not df.empty else [],
            "locations": sorted(df["location"].dropna().unique().tolist()) if not df.empty else [],
        },
    }
    return jsonify(data)


def _parse_date(s: str):
    return datetime.strptime(s, "%Y-%m-%d").date()


def _to_dataframe(query) -> pd.DataFrame:
    rows = query.all()
    if not rows:
        return pd.DataFrame()
    records = []
    for r in rows:
        records.append(
            {
                "date": r.date,
                "job_name": r.job_name,
                "shift_name": r.shift_name,
                "full_name": r.full_name,
                "location": r.location,
                "site": r.site,
                "role": r.role,
                "month": r.month,
                "day": r.day,
                "shift_start": r.shift_start,
                "shift_end": r.shift_end,
                "duration": r.duration,
                "paid_hours": r.paid_hours,
                "hour_rate": r.hour_rate,
                "deductions": r.deductions,
                "additions": r.additions,
                "total_pay": r.total_pay,
                "client_hourly_rate": r.client_hourly_rate,
                "client_net": r.client_net,
                "client": r.client,
                "dns": r.dns,
                "job_status": r.job_status,
            }
        )
    return pd.DataFrame.from_records(records)
