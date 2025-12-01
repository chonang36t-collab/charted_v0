"""Main routes blueprint: dashboard, upload, and API for analysis."""
from __future__ import annotations
from sqlalchemy import text
from app import cache

import io
from datetime import datetime
from typing import List, Optional

import pandas as pd
from flask import (
    Blueprint,
    current_app,
    jsonify,
    request,
)
from flask_login import login_required
from sqlalchemy import func, case

from . import db
from .models import FactShift, DimClient, DimDate
from .auth import admin_required
from .models import FactShift, DimEmployee, DimClient, DimJob, DimDate, DimShift
from .utils.data_loader import dbDataLoader

main_bp = Blueprint("main", __name__)

# Define required columns for the new structure
REQUIRED_COLUMNS = [
    'job_name', 'shift_name', 'full_name', 'location', 'site', 'role', 
    'month', 'date', 'day', 'shift_start', 'shift_end', 'duration', 
    'paid_hours', 'hour_rate', 'deductions', 'additions', 'total_pay', 
    'client_hourly_rate', 'client_net', 'self_employed', 'dns', 'client', 'job_status'
]

@main_bp.route("/api/upload/required-columns")
@login_required
@admin_required
def api_upload_required_columns():
    return jsonify({"required_columns": sorted(REQUIRED_COLUMNS)})

@main_bp.route("/api/upload", methods=["POST"])
@login_required
@admin_required
def api_upload():
    file = request.files.get("file")
    if not file or file.filename == "":
        return jsonify({"error": "No file provided."}), 400

    ext = file.filename.rsplit(".", 1)[-1].lower()
    if ext not in current_app.config.get("ALLOWED_EXTENSIONS", {"xlsx"}):
        return jsonify({"error": "Only .xlsx files are allowed."}), 400

    try:
        import tempfile
        import os
        
        with tempfile.NamedTemporaryFile(delete=False, suffix='.xlsx') as tmp_file:
            file.save(tmp_file.name)
            tmp_path = tmp_file.name

        loader = dbDataLoader()
        success = loader.load_excel_data(tmp_path)
        
        os.unlink(tmp_path)
        
        if success:
            return jsonify({
                "message": "Upload complete.",
                "details": {
                    "inserted": "Data processed successfully",
                    "duplicates": 0,
                    "skipped": 0,
                },
            })
        else:
            return jsonify({"error": "Failed to process data"}), 500
            
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as e:
        current_app.logger.exception("Upload failed")
        return jsonify({"error": f"Unexpected error during upload: {str(e)}"}), 500

@main_bp.route("/api/metrics")
@login_required
def api_metrics():
    query = db.session.query(
        FactShift,
        DimEmployee.full_name,
        DimClient.client_name,
        DimJob.location,
        DimJob.job_name,
        DimDate.date,
        DimDate.month,
        DimDate.day
    ).join(
        DimEmployee, FactShift.employee_id == DimEmployee.employee_id
    ).join(
        DimClient, FactShift.client_id == DimClient.client_id
    ).join(
        DimJob, FactShift.job_id == DimJob.job_id
    ).join(
        DimDate, FactShift.date_id == DimDate.date_id
    )

    start = request.args.get("start")
    end = request.args.get("end")
    full_names = request.args.getlist("full_names")
    clients = request.args.getlist("clients")
    locations = request.args.getlist("locations")

    if start:
        query = query.filter(DimDate.date >= start)
    if end:
        query = query.filter(DimDate.date <= end)
    if full_names:
        query = query.filter(DimEmployee.full_name.in_(full_names))
    if clients:
        query = query.filter(DimClient.client_name.in_(clients))
    if locations:
        query = query.filter(DimJob.location.in_(locations))

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

@main_bp.route("/api/sales-summary/totals")
@login_required
def api_sales_summary_totals():
    """Get current and previous period totals"""
    try:
        from sqlalchemy import distinct
        from datetime import datetime, timedelta
        
        start = request.args.get("start")
        end = request.args.get("end")
        
        if not start or not end:
            return jsonify({"error": "Start and end dates are required"}), 400

        start_date = datetime.strptime(start, '%Y-%m-%d')
        end_date = datetime.strptime(end, '%Y-%m-%d')
        days_diff = (end_date - start_date).days
        
        # Current period totals
        current_totals_query = db.session.query(
            func.sum(FactShift.client_net).label('total_revenue'),
            func.sum(FactShift.total_pay).label('total_cost'),
            func.count(distinct(FactShift.client_id)).label('total_clients'),
            func.count(FactShift.shift_record_id).label('total_shifts'),
            func.count(distinct(FactShift.employee_id)).label('unique_employees'),
            func.sum(FactShift.paid_hours).label('total_paid_hours')
        ).join(DimDate, FactShift.date_id == DimDate.date_id)
        
        current_totals_query = current_totals_query.filter(
            DimDate.date >= start,
            DimDate.date <= end
        )
        
        current_totals = current_totals_query.first()
        
        # Previous period totals
        previous_start = start_date - timedelta(days=days_diff + 1)
        previous_end = start_date - timedelta(days=1)
        
        previous_totals_query = db.session.query(
            func.sum(FactShift.client_net).label('prev_revenue'),
            func.sum(FactShift.total_pay).label('prev_cost'),
            func.count(distinct(FactShift.client_id)).label('prev_clients'),
            func.count(FactShift.shift_record_id).label('prev_shifts'),
            func.count(distinct(FactShift.employee_id)).label('prev_employees'),
            func.sum(FactShift.paid_hours).label('prev_paid_hours')
        ).join(DimDate, FactShift.date_id == DimDate.date_id)
        
        previous_totals_query = previous_totals_query.filter(
            DimDate.date >= previous_start.strftime('%Y-%m-%d'),
            DimDate.date <= previous_end.strftime('%Y-%m-%d')
        )
        previous_totals = previous_totals_query.first()
        
        # Calculate metrics
        current_revenue = float(current_totals[0] or 0)
        current_cost = float(current_totals[1] or 0)
        current_paid_hours = float(current_totals[5] or 0)
        
        current_profit_margin = ((current_revenue - current_cost) / current_revenue * 100) if current_revenue > 0 else 0
        current_avg_client_pay = (current_revenue / current_paid_hours) if current_paid_hours > 0 else 0
        current_avg_staff_pay = (current_cost / current_paid_hours) if current_paid_hours > 0 else 0
        
        previous_revenue = float(previous_totals[0] or 0)
        previous_cost = float(previous_totals[1] or 0)
        previous_paid_hours = float(previous_totals[5] or 0)
        
        previous_profit_margin = ((previous_revenue - previous_cost) / previous_revenue * 100) if previous_revenue > 0 else 0
        previous_avg_client_pay = (previous_revenue / previous_paid_hours) if previous_paid_hours > 0 else 0
        previous_avg_staff_pay = (previous_cost / previous_paid_hours) if previous_paid_hours > 0 else 0
        
        payload = {
            "current": {
                "totalRevenue": round(current_revenue, 2),
                "totalCost": round(current_cost, 2),
                "totalShifts": current_totals[3] or 0,
                "totalClients": current_totals[2] or 0,
                "uniqueEmployees": current_totals[4] or 0,
                "totalPaidHours": round(current_paid_hours, 2),
                "avgClientPayPerHour": round(current_avg_client_pay, 2),
                "avgStaffPayPerHour": round(current_avg_staff_pay, 2),
                "profitMargin": round(current_profit_margin, 1),
            },
            "previous": {
                "totalRevenue": round(previous_revenue, 2),
                "totalCost": round(previous_cost, 2),
                "totalShifts": previous_totals[3] or 0,
                "totalClients": previous_totals[2] or 0,
                "uniqueEmployees": previous_totals[4] or 0,
                "totalPaidHours": round(previous_paid_hours, 2),
                "avgClientPayPerHour": round(previous_avg_client_pay, 2),
                "avgStaffPayPerHour": round(previous_avg_staff_pay, 2),
                "profitMargin": round(previous_profit_margin, 1),
            }
        }
        
        return jsonify(payload)
        
    except Exception as e:
        current_app.logger.error(f"Error in sales-summary-totals API: {e}")
        return jsonify({
            "current": {
                "totalRevenue": 0.0, "totalCost": 0.0, "totalShifts": 0, "totalClients": 0,
                "uniqueEmployees": 0, "totalPaidHours": 0.0, "avgClientPayPerHour": 0.0,
                "avgStaffPayPerHour": 0.0, "profitMargin": 0.0
            },
            "previous": {
                "totalRevenue": 0.0, "totalCost": 0.0, "totalShifts": 0, "totalClients": 0,
                "uniqueEmployees": 0, "totalPaidHours": 0.0, "avgClientPayPerHour": 0.0,
                "avgStaffPayPerHour": 0.0, "profitMargin": 0.0
            }
        })

@main_bp.route("/api/sales-summary/timeseries")
@login_required
def api_sales_summary_timeseries():
    """Get time series data with smart aggregation"""
    try:
        from datetime import datetime, timedelta
        
        start = request.args.get("start")
        end = request.args.get("end")
        
        if not start or not end:
            return jsonify({"error": "Start and end dates are required"}), 400

        start_date = datetime.strptime(start, '%Y-%m-%d')
        end_date = datetime.strptime(end, '%Y-%m-%d')
        days_diff = (end_date - start_date).days
        
        # Determine aggregation level
        if days_diff > 180:  # >6 months - monthly
            # Cast to date type for PostgreSQL date functions
            period_format = func.to_char(func.cast(DimDate.date, db.Date), 'YYYY-MM')
            period_display = func.to_char(func.cast(DimDate.date, db.Date), 'Mon YYYY')
            aggregation_level = "monthly"
        elif days_diff > 30:  # 1-6 months - weekly
            period_format = func.date_trunc('week', func.cast(DimDate.date, db.Date))
            period_display = func.to_char(func.date_trunc('week', func.cast(DimDate.date, db.Date)), 'DD Mon')
            aggregation_level = "weekly"
        else:  # <1 month - daily
            period_format = DimDate.date
            period_display = func.to_char(func.cast(DimDate.date, db.Date), 'DD Mon')
            aggregation_level = "daily"
        
        # Time series query
        time_series_query = db.session.query(
            period_format.label('period'),
            period_display.label('display'),
            func.sum(FactShift.client_net).label('revenue'),
            func.sum(FactShift.total_pay).label('cost')
        ).join(DimDate, FactShift.date_id == DimDate.date_id)
        
        time_series_query = time_series_query.filter(
            DimDate.date >= start,
            DimDate.date <= end
        )
        
        # Group and order based on aggregation level
        if aggregation_level == "daily":
            time_series_results = time_series_query.group_by(DimDate.date, period_display).order_by(DimDate.date).all()
        else:
            time_series_results = time_series_query.group_by(period_format, period_display).order_by(period_format).all()
        
        time_series_data = [
            {
                "period": str(period),
                "display": display,
                "revenue": round(float(revenue or 0), 2),
                "cost": round(float(cost or 0), 2)
            }
            for period, display, revenue, cost in time_series_results
        ]
        
        return jsonify({
            "timeSeries": time_series_data,
            "aggregationLevel": aggregation_level
        })
        
    except Exception as e:
        current_app.logger.error(f"Error in sales-summary-timeseries API: {e}")
        return jsonify({
            "timeSeries": [],
            "aggregationLevel": "daily"
        })

@main_bp.route("/api/sales-summary")
@login_required
@cache.cached(timeout=300, query_string=True)
def api_sales_summary_combined():
    """Combined endpoint that calls both totals and timeseries"""
    try:
        start = request.args.get("start")
        end = request.args.get("end")
        
        if not start or not end:
            return jsonify({"error": "Start and end dates are required"}), 400
        
        # Get totals data directly (no ThreadPoolExecutor)
        from sqlalchemy import distinct
        from datetime import datetime, timedelta
        
        start_date = datetime.strptime(start, '%Y-%m-%d')
        end_date = datetime.strptime(end, '%Y-%m-%d')
        days_diff = (end_date - start_date).days
        
        # Current period totals
        current_totals_query = db.session.query(
            func.sum(FactShift.client_net).label('total_revenue'),
            func.sum(FactShift.total_pay).label('total_cost'),
            func.count(distinct(FactShift.client_id)).label('total_clients'),
            func.count(FactShift.shift_record_id).label('total_shifts'),
            func.count(distinct(FactShift.employee_id)).label('unique_employees'),
            func.sum(FactShift.paid_hours).label('total_paid_hours')
        ).join(DimDate, FactShift.date_id == DimDate.date_id)
        
        current_totals_query = current_totals_query.filter(
            DimDate.date >= start,
            DimDate.date <= end
        )
        
        current_totals = current_totals_query.first()
        
        # Previous period totals
        previous_start = start_date - timedelta(days=days_diff + 1)
        previous_end = start_date - timedelta(days=1)
        
        previous_totals_query = db.session.query(
            func.sum(FactShift.client_net).label('prev_revenue'),
            func.sum(FactShift.total_pay).label('prev_cost'),
            func.count(distinct(FactShift.client_id)).label('prev_clients'),
            func.count(FactShift.shift_record_id).label('prev_shifts'),
            func.count(distinct(FactShift.employee_id)).label('prev_employees'),
            func.sum(FactShift.paid_hours).label('prev_paid_hours')
        ).join(DimDate, FactShift.date_id == DimDate.date_id)
        
        previous_totals_query = previous_totals_query.filter(
            DimDate.date >= previous_start.strftime('%Y-%m-%d'),
            DimDate.date <= previous_end.strftime('%Y-%m-%d')
        )
        previous_totals = previous_totals_query.first()
        
        # Calculate metrics for current period
        current_revenue = float(current_totals[0] or 0)
        current_cost = float(current_totals[1] or 0)
        current_paid_hours = float(current_totals[5] or 0)
        
        current_profit_margin = ((current_revenue - current_cost) / current_revenue * 100) if current_revenue > 0 else 0
        current_avg_client_pay = (current_revenue / current_paid_hours) if current_paid_hours > 0 else 0
        current_avg_staff_pay = (current_cost / current_paid_hours) if current_paid_hours > 0 else 0
        
        # Calculate metrics for previous period
        previous_revenue = float(previous_totals[0] or 0)
        previous_cost = float(previous_totals[1] or 0)
        previous_paid_hours = float(previous_totals[5] or 0)
        
        previous_profit_margin = ((previous_revenue - previous_cost) / previous_revenue * 100) if previous_revenue > 0 else 0
        previous_avg_client_pay = (previous_revenue / previous_paid_hours) if previous_paid_hours > 0 else 0
        previous_avg_staff_pay = (previous_cost / previous_paid_hours) if previous_paid_hours > 0 else 0
        
        # Time series data
        if days_diff > 180:
            period_format = func.to_char(func.cast(DimDate.date, db.Date), 'YYYY-MM')
            period_display = func.to_char(func.cast(DimDate.date, db.Date), 'Mon YYYY')
            aggregation_level = "monthly"
        elif days_diff > 30:
            period_format = func.date_trunc('week', func.cast(DimDate.date, db.Date))
            period_display = func.to_char(func.date_trunc('week', func.cast(DimDate.date, db.Date)), 'DD Mon')
            aggregation_level = "weekly"
        else:
            period_format = DimDate.date
            period_display = func.to_char(func.cast(DimDate.date, db.Date), 'DD Mon')
            aggregation_level = "daily"
        
        time_series_query = db.session.query(
            period_format.label('period'),
            period_display.label('display'),
            func.sum(FactShift.client_net).label('revenue'),
            func.sum(FactShift.total_pay).label('cost')
        ).join(DimDate, FactShift.date_id == DimDate.date_id)
        
        time_series_query = time_series_query.filter(
            DimDate.date >= start,
            DimDate.date <= end
        )
        
        if aggregation_level == "daily":
            time_series_results = time_series_query.group_by(DimDate.date, period_display).order_by(DimDate.date).all()
        else:
            time_series_results = time_series_query.group_by(period_format, period_display).order_by(period_format).all()
        
        time_series_data = [
            {
                "period": str(period),
                "display": display,
                "revenue": round(float(revenue or 0), 2),
                "cost": round(float(cost or 0), 2)
            }
            for period, display, revenue, cost in time_series_results
        ]
        
        # Build final response
        payload = {
            "totalRevenue": round(current_revenue, 2),
            "totalCost": round(current_cost, 2),
            "totalShifts": current_totals[3] or 0,
            "totalClients": current_totals[2] or 0,
            "uniqueEmployees": current_totals[4] or 0,
            "totalPaidHours": round(current_paid_hours, 2),
            "avgClientPayPerHour": round(current_avg_client_pay, 2),
            "avgStaffPayPerHour": round(current_avg_staff_pay, 2),
            "profitMargin": round(current_profit_margin, 1),
            "previousPeriod": {
                "totalRevenue": round(previous_revenue, 2),
                "totalCost": round(previous_cost, 2),
                "totalShifts": previous_totals[3] or 0,
                "totalClients": previous_totals[2] or 0,
                "uniqueEmployees": previous_totals[4] or 0,
                "totalPaidHours": round(previous_paid_hours, 2),
                "avgClientPayPerHour": round(previous_avg_client_pay, 2),
                "avgStaffPayPerHour": round(previous_avg_staff_pay, 2),
                "profitMargin": round(previous_profit_margin, 1),
            },
            "timeSeries": time_series_data,
            "aggregationLevel": aggregation_level
        }
        
        return jsonify(payload)
        
    except Exception as e:
        current_app.logger.error(f"Error in combined sales-summary API: {e}")
        import traceback
        print(f"ERROR: {traceback.format_exc()}")
        return jsonify({
            "totalRevenue": 0.0, "totalCost": 0.0, "totalShifts": 0, "totalClients": 0,
            "uniqueEmployees": 0, "totalPaidHours": 0.0, "avgClientPayPerHour": 0.0,
            "avgStaffPayPerHour": 0.0, "profitMargin": 0.0,
            "previousPeriod": {
                "totalRevenue": 0.0, "totalCost": 0.0, "totalShifts": 0, "totalClients": 0,
                "uniqueEmployees": 0, "totalPaidHours": 0.0, "avgClientPayPerHour": 0.0,
                "avgStaffPayPerHour": 0.0, "profitMargin": 0.0
            },
            "timeSeries": [],
            "aggregationLevel": "daily"
        })
        
# Helper functions
def _to_dataframe(query) -> pd.DataFrame:
    rows = query.all()
    if not rows:
        return pd.DataFrame()
    
    records = []
    for row in rows:
        fact_shift = row[0]
        records.append({
            "date": row[5],
            "job_name": row[4],
            "shift_name": fact_shift.shift.shift_name if fact_shift.shift else None,
            "full_name": row[1],
            "location": row[3],
            "site": fact_shift.job.site if fact_shift.job else None,
            "role": fact_shift.employee.role if fact_shift.employee else None,
            "month": row[6],
            "day": row[7],
            "shift_start": fact_shift.shift.shift_start if fact_shift.shift else None,
            "shift_end": fact_shift.shift.shift_end if fact_shift.shift else None,
            "duration": fact_shift.duration,
            "paid_hours": fact_shift.paid_hours,
            "hour_rate": fact_shift.hour_rate,
            "deductions": fact_shift.deductions,
            "additions": fact_shift.additions,
            "total_pay": fact_shift.total_pay,
            "client_hourly_rate": fact_shift.client_hourly_rate,
            "client_net": fact_shift.client_net,
            "client": row[2],
            "dns": fact_shift.dns,
            "job_status": fact_shift.job_status,
        })
    return pd.DataFrame.from_records(records)

def compute_kpis(df):
    if df.empty:
        return {}
    
    return {
        "total_revenue": float(df["client_net"].sum()),
        "total_cost": float(df["total_pay"].sum()),
        "total_profit": float((df["client_net"] - df["total_pay"]).sum()),
        "total_hours": float(df["paid_hours"].sum()),
        "profit_margin": float(((df["client_net"] - df["total_pay"]).sum() / df["client_net"].sum() * 100) if df["client_net"].sum() > 0 else 0),
    }

def timeseries_by(df, freq="D"):
    if df.empty or "date" not in df:
        return []
    
    try:
        df['date'] = pd.to_datetime(df['date'])
        df_ts = df.groupby(pd.Grouper(key='date', freq=freq)).agg({
            'client_net': 'sum',
            'total_pay': 'sum'
        }).reset_index()
        
        return [
            {
                "date": row['date'].strftime('%Y-%m-%d'),
                "revenue": float(row['client_net']),
                "cost": float(row['total_pay'])
            }
            for _, row in df_ts.iterrows()
        ]
    except:
        return []

def top_n_clients(df, n=10):
    if df.empty:
        return []
    
    top_clients = df.groupby('client')['client_net'].sum().nlargest(n)
    return [
        {"client": client, "revenue": float(revenue)}
        for client, revenue in top_clients.items()
    ]

def top_n_locations(df, n=10):
    if df.empty:
        return []
    
    top_locations = df.groupby('location')['client_net'].sum().nlargest(n)
    return [
        {"location": loc, "revenue": float(revenue)}
        for loc, revenue in top_locations.items()
    ]

def hours_distribution(df):
    if df.empty:
        return []
    
    return [
        {"range": "0-4", "count": int(((df['paid_hours'] >= 0) & (df['paid_hours'] <= 4)).sum())},
        {"range": "5-8", "count": int(((df['paid_hours'] > 4) & (df['paid_hours'] <= 8)).sum())},
        {"range": "9-12", "count": int(((df['paid_hours'] > 8) & (df['paid_hours'] <= 12)).sum())},
        {"range": "12+", "count": int((df['paid_hours'] > 12).sum())},
    ]

def summary_stats(df):
    if df.empty:
        return {}
    
    return {
        "avg_hourly_rate": float(df['hour_rate'].mean()),
        "avg_client_rate": float(df['client_hourly_rate'].mean()),
        "total_shifts": len(df),
        "unique_employees": df['full_name'].nunique(),
        "unique_clients": df['client'].nunique(),
    }

@main_bp.route("/api/filters")
@login_required
def api_filters():
    """Get available filters (clients, locations, sites)"""
    try:
        # Get all clients
        clients = [c.client_name for c in DimClient.query.with_entities(DimClient.client_name).distinct().order_by(DimClient.client_name).all()]
        
        # Get locations and their sites
        jobs = DimJob.query.with_entities(DimJob.location, DimJob.site).distinct().all()
        
        locations_map = {}
        for loc, site in jobs:
            if loc:
                if loc not in locations_map:
                    locations_map[loc] = set()
                if site:
                    locations_map[loc].add(site)
        
        locations_data = []
        for loc in sorted(locations_map.keys()):
            locations_data.append({
                "name": loc,
                "sites": sorted(list(locations_map[loc]))
            })
            
        return jsonify({
            "clients": clients,
            "locations": locations_data
        })
        
    except Exception as e:
        current_app.logger.error(f"Error in filters API: {e}")
        return jsonify({"error": str(e)}), 500

@main_bp.route("/api/chart-data")
@login_required
def api_chart_data():
    """Get aggregated chart data based on dimension and metrics"""
    try:
        start = request.args.get("start")
        end = request.args.get("end")
        dimension = request.args.get("dimension", "").lower()
        metrics = [m.lower() for m in request.args.getlist("metrics")]
        
        # Filter parameters
        filter_clients = request.args.getlist("clients")
        filter_locations = request.args.getlist("locations")
        filter_sites = request.args.getlist("sites")

        if not start or not end or not dimension or not metrics:
            return jsonify({"error": "Missing required parameters"}), 400

        # Map dimension to model column
        dim_map = {
            "date": DimDate.date,
            "month": DimDate.month,
            "year": DimDate.year,
            "client_name": DimClient.client_name,
            "full_name": DimEmployee.full_name,
            "role": DimEmployee.role,
            "job_name": DimJob.job_name,
            "location": DimJob.location,
        }

        # Map metric to model column
        metric_map = {
            "revenue": func.sum(FactShift.client_net),
            "cost": func.sum(FactShift.total_pay),
            "profit": func.sum(FactShift.client_net - FactShift.total_pay),
            "profit_margin": case(
                (func.sum(FactShift.client_net) > 0, 
                 func.sum(FactShift.client_net - FactShift.total_pay) / func.sum(FactShift.client_net) * 100),
                else_=0
            ),
            "total_shifts": func.count(FactShift.shift_record_id),
            "paid_hours": func.sum(FactShift.paid_hours),
            "duration": func.sum(FactShift.duration),
            "hourly_rate": func.avg(FactShift.hour_rate),
        }

        if dimension not in dim_map:
            return jsonify({"error": f"Invalid dimension: {dimension}"}), 400
        
        # Validate metrics
        valid_metrics = []
        for m in metrics:
            if m in metric_map:
                valid_metrics.append(m)
        
        if not valid_metrics:
            return jsonify({"error": "No valid metrics provided"}), 400

        dim_col = dim_map[dimension]
        metric_cols = [metric_map[m].label(m) for m in valid_metrics]

        query = db.session.query(
            dim_col.label("name"),
            *metric_cols
        ).join(
            DimDate, FactShift.date_id == DimDate.date_id
        )

        # Join other tables if needed for dimensions OR filters
        joined_tables = set()
        
        # Helper to join table if not already joined
        def ensure_join(table, condition):
            if table not in joined_tables:
                nonlocal query
                query = query.join(table, condition)
                joined_tables.add(table)

        # Handle Dimension Joins
        if dimension == "client_name":
            ensure_join(DimClient, FactShift.client_id == DimClient.client_id)
        elif dimension in ["full_name", "role"]:
            ensure_join(DimEmployee, FactShift.employee_id == DimEmployee.employee_id)
        elif dimension in ["job_name", "location"]:
            ensure_join(DimJob, FactShift.job_id == DimJob.job_id)
            
        # Handle Filter Joins
        if filter_clients:
            ensure_join(DimClient, FactShift.client_id == DimClient.client_id)
            query = query.filter(DimClient.client_name.in_(filter_clients))
            
        if filter_locations or filter_sites:
            ensure_join(DimJob, FactShift.job_id == DimJob.job_id)
            
            if filter_locations:
                query = query.filter(DimJob.location.in_(filter_locations))
            
            if filter_sites:
                query = query.filter(DimJob.site.in_(filter_sites))

        query = query.filter(
            DimDate.date >= start,
            DimDate.date <= end
        ).group_by(dim_col)
        
        # For month dimension, order by the minimum date in each month group for chronological order
        if dimension == "month":
            query = query.order_by(func.min(DimDate.date))
        else:
            query = query.order_by(dim_col)

        results = query.all()

        # Month abbreviation mapping
        month_abbrev = {
            "January": "Jan", "February": "Feb", "March": "Mar",
            "April": "Apr", "May": "May", "June": "Jun",
            "July": "Jul", "August": "Aug", "September": "Sep",
            "October": "Oct", "November": "Nov", "December": "Dec"
        }

        data = []
        for row in results:
            # Abbreviate month names if dimension is month
            name_value = str(row.name)
            if dimension == "month" and name_value in month_abbrev:
                name_value = month_abbrev[name_value]
            
            item = {"name": name_value}
            for m in valid_metrics:
                val = getattr(row, m)
                item[m] = round(float(val or 0), 2)
            data.append(item)

        return jsonify(data)

    except Exception as e:
        current_app.logger.error(f"Error in chart-data API: {e}")
        return jsonify({"error": str(e)}), 500