from __future__ import annotations

from collections import defaultdict
from typing import Dict, List

from flask import jsonify, request
from flask_login import login_required
from sqlalchemy import func, text

# ✅ FIXED: Import db from the main app module, not relative
from app import db
from app.models import FactShift, DimEmployee, DimClient, DimJob, DimShift, DimDate
from . import advanced_reports_bp

# Helpers - Updated to work with new normalized structure
NUMERIC_COLS = [
    "duration",
    "paid_hours",
    "hour_rate",
    "deductions",
    "additions",
    "total_pay",
    "client_hourly_rate",
    "client_net",
]
CATEGORICAL_COLS = [
    "date",
    "job_name",
    "shift_name",
    "full_name",
    "location",
    "site",
    "role",
    "month",
    "day",
    "client",
    "job_status",
]

@advanced_reports_bp.route("/api/reports/columns")
@login_required
def api_columns():
    return jsonify({
        "numeric": NUMERIC_COLS,
        "categorical": CATEGORICAL_COLS,
    })

@advanced_reports_bp.route("/api/reports/data")
@login_required
def api_data():
    chart_type = request.args.get("chartType")
    x_col = request.args.get("x")
    y_col = request.args.get("y")

    # Basic validation
    if not x_col or not y_col or not chart_type:
        return jsonify({"error": "Missing required parameters: x, y, chartType"}), 400
    if y_col not in NUMERIC_COLS:
        return jsonify({"error": "Y column must be numeric"}), 400
    if x_col not in (NUMERIC_COLS + CATEGORICAL_COLS):
        return jsonify({"error": "Invalid X column"}), 400

    # Build base query with joins for normalized structure
    query = db.session.query(
        FactShift,
        DimEmployee.full_name,
        DimClient.client_name,
        DimJob.job_name,
        DimJob.location,
        DimJob.site,
        DimEmployee.role,
        DimDate.date,
        DimDate.month,
        DimDate.day,
        DimShift.shift_name
    ).join(
        DimEmployee, FactShift.employee_id == DimEmployee.employee_id
    ).join(
        DimClient, FactShift.client_id == DimClient.client_id
    ).join(
        DimJob, FactShift.job_id == DimJob.job_id
    ).join(
        DimDate, FactShift.date_id == DimDate.date_id
    ).join(
        DimShift, FactShift.shift_id == DimShift.shift_id
    )

    # Map column names to their actual sources
    column_mapping = {
        # Numeric columns (from FactShift)
        "duration": FactShift.duration,
        "paid_hours": FactShift.paid_hours,
        "hour_rate": FactShift.hour_rate,
        "deductions": FactShift.deductions,
        "additions": FactShift.additions,
        "total_pay": FactShift.total_pay,
        "client_hourly_rate": FactShift.client_hourly_rate,
        "client_net": FactShift.client_net,
        
        # Categorical columns (from dimension tables)
        "date": DimDate.date,
        "job_name": DimJob.job_name,
        "shift_name": DimShift.shift_name,
        "full_name": DimEmployee.full_name,
        "location": DimJob.location,
        "site": DimJob.site,
        "role": DimEmployee.role,
        "month": DimDate.month,
        "day": DimDate.day,
        "client": DimClient.client_name,
        "job_status": FactShift.job_status,
    }

    # Get the actual column objects for X and Y
    x_column = column_mapping.get(x_col)
    y_column = column_mapping.get(y_col)

    if not x_column or not y_column:
        return jsonify({"error": "Invalid column selection"}), 400

    # Choose aggregation for y
    agg = func.sum(y_column)
    
    # Build the final query with grouping
    final_query = db.session.query(
        x_column.label("x"),
        agg.label("y")
    ).select_from(
        query.subquery()  # Use the joined query as base
    ).group_by(x_column)

    # Optional filters
    filter_mapping = {
        "site": DimJob.site,
        "role": DimEmployee.role, 
        "month": DimDate.month,
        "location": DimJob.location,
        "client": DimClient.client_name
    }
    
    for filt, column in filter_mapping.items():
        val = request.args.get(filt)
        if val:
            final_query = final_query.filter(column == val)

    # Execute and format
    try:
        rows = final_query.order_by(x_column).all()
        labels = []
        values = []
        
        for xv, yv in rows:
            # For date objects, cast to string
            labels.append(str(xv) if xv is not None else "Unknown")
            values.append(float(yv or 0))

        return jsonify({"labels": labels, "values": values})
        
    except Exception as e:
        return jsonify({"error": f"Database query failed: {str(e)}"}), 500
