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
    send_from_directory,
)
import os
from flask_login import login_required, current_user
from sqlalchemy import func, case, desc

from . import db
from .models import FactShift, DimClient, DimDate
from .auth import admin_required, manager_required
from .models import FactShift, DimEmployee, DimClient, DimJob, DimDate, DimShift
from .utils.data_loader import dbDataLoader

main_bp = Blueprint("main", __name__)

@main_bp.route("/", defaults={"path": ""})
@main_bp.route("/<path:path>")
def serve(path):
    if path != "" and os.path.exists(os.path.join(current_app.static_folder, path)):
        return send_from_directory(current_app.static_folder, path)
    return send_from_directory(current_app.static_folder, "index.html")

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

    import tempfile
    import os
    import json
    from flask import Response, stream_with_context
    
    # Save file temporarily
    fd, tmp_path = tempfile.mkstemp(suffix='.xlsx')
    os.close(fd)
    file.save(tmp_path)

    def generate():
        try:
            loader = dbDataLoader()
            # Iterate over the generator from data_loader
            for progress_data in loader.load_excel_data(tmp_path):
                yield json.dumps(progress_data) + '\n'
        except Exception as e:
            yield json.dumps({"status": "error", "message": str(e)}) + '\n'
        finally:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)

    return Response(stream_with_context(generate()), mimetype='application/x-ndjson')

@main_bp.route("/api/locations")
@login_required
def api_get_locations():
    """Get unique list of job locations"""
    try:
        locations = db.session.query(DimJob.location).distinct().all()
        return jsonify([loc[0] for loc in locations if loc[0]])
    except Exception as e:
        current_app.logger.error(f"Error fetching locations: {e}")
        return jsonify([]), 500

@main_bp.route("/api/sites")
@login_required
def api_get_sites():
    """Get unique list of sites, optionally filtered by location and role"""
    try:
        # Use DimJob as the base
        query = db.session.query(DimJob.site).distinct()
        
        # apply_dashboard_filters will handle:
        # 1. Role-based restrictions (Managers only see their locations)
        # 2. Filtering by specific 'locations' if passed in query params
        query = apply_dashboard_filters(query)
        
        # Additional cleanup: filter out nulls/empties if they exist
        query = query.filter(DimJob.site != None, DimJob.site != '')
        
        sites = query.order_by(DimJob.site).all()
        return jsonify([s[0] for s in sites])
    except Exception as e:
        current_app.logger.error(f"Error fetching sites: {e}")
        # Return empty list on error
        return jsonify([]), 200

@main_bp.route("/api/rankings/staff")
@login_required
def api_rankings_staff():
    """Get top staff by paid hours"""
    try:
        start = request.args.get("start")
        end = request.args.get("end")
        limit = int(request.args.get("limit", 10))
        
        query = db.session.query(
            DimEmployee.full_name,
            func.sum(FactShift.paid_hours).label('value')
        ).join(
            DimEmployee, FactShift.employee_id == DimEmployee.employee_id
        ).join(
            DimDate, FactShift.date_id == DimDate.date_id
        )
        
        if start and end:
            query = query.filter(DimDate.date >= start, DimDate.date <= end)
            
        # Apply role/location/site filters
        query = apply_dashboard_filters(query)
        
        results = query.group_by(DimEmployee.full_name)\
            .order_by(desc('value'))\
            .limit(limit).all()
            
        return jsonify([
            {"name": r[0], "value": float(r[1] or 0)} 
            for r in results
        ])
    except Exception as e:
        current_app.logger.error(f"Error fetching staff rankings: {e}")
        return jsonify([]), 500

@main_bp.route("/api/rankings/clients")
@login_required
def api_rankings_clients():
    """Get top clients by revenue or shifts"""
    try:
        start = request.args.get("start")
        end = request.args.get("end")
        limit = int(request.args.get("limit", 5))
        metric = request.args.get("metric", "revenue") # revenue or shifts
        
        if metric == 'revenue':
            value_col = func.sum(FactShift.client_net).label('value')
        else:
            value_col = func.count(FactShift.shift_record_id).label('value')
            
        query = db.session.query(
            DimClient.client_name,
            value_col
        ).join(
            DimClient, FactShift.client_id == DimClient.client_id
        ).join(
            DimDate, FactShift.date_id == DimDate.date_id
        )
        
        if start and end:
            query = query.filter(DimDate.date >= start, DimDate.date <= end)
            
        # Apply role/location/site filters
        query = apply_dashboard_filters(query)
        
        results = query.group_by(DimClient.client_name)\
            .order_by(desc('value'))\
            .limit(limit).all()
            
        return jsonify([
            {"name": r[0], "value": float(r[1] or 0)} 
            for r in results
        ])
    except Exception as e:
        current_app.logger.error(f"Error fetching client rankings: {e}")
        return jsonify([]), 500

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
        # Apply role-based location filtering
        current_totals_query = apply_location_filter(current_totals_query)
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
        
        # Apply role-based location filtering
        current_totals_query = apply_dashboard_filters(current_totals_query)
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
        
        # Apply role-based location filtering
        previous_totals_query = apply_dashboard_filters(previous_totals_query)
        previous_totals = previous_totals_query.first()
        
        if current_user.role != 'admin':
             # Sanitize ONLY financial metrics for non-admin roles
             # But keep operational metrics (clients, shifts, employees, hours)
             current_revenue = 0.0
             current_cost = 0.0
             current_profit_margin = 0.0
             current_avg_client_pay = 0.0
             current_avg_staff_pay = 0.0
             
             # Extract operational data
             current_paid_hours = float(current_totals[5] or 0)
             
             previous_revenue = 0.0
             previous_cost = 0.0
             previous_profit_margin = 0.0
             previous_avg_client_pay = 0.0
             previous_avg_staff_pay = 0.0
             previous_paid_hours = float(previous_totals[5] or 0)
        else:
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
            func.sum(FactShift.total_pay).label('cost'),
            func.sum(FactShift.paid_hours).label('paid_hours'),
            func.count(FactShift.shift_record_id).label('shifts')
        ).join(DimDate, FactShift.date_id == DimDate.date_id)
        
        time_series_query = time_series_query.filter(
            DimDate.date >= start,
            DimDate.date <= end
        )
        
        # Apply role-based location filtering
        time_series_query = apply_dashboard_filters(time_series_query)
        
        if aggregation_level == "daily":
            time_series_results = time_series_query.group_by(DimDate.date, period_display).order_by(DimDate.date).all()
        else:
            time_series_results = time_series_query.group_by(period_format, period_display).order_by(period_format).all()
        
        time_series_data = [
            {
                "period": str(period),
                "display": display,
                "revenue": round(float(revenue or 0), 2) if current_user.role == 'admin' else 0.0,
                "cost": round(float(cost or 0), 2) if current_user.role == 'admin' else 0.0,
                "paidHours": round(float(paid_hours or 0), 2),
                "totalShifts": int(shifts or 0)
            }
            for period, display, revenue, cost, paid_hours, shifts in time_series_results
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
        

from .utils.filters import apply_dashboard_filters

@main_bp.route("/api/financial-summary")
@login_required
@admin_required
def api_financial_summary():
    """Admin-only summary with full financial data"""
    return api_sales_summary_combined()

@main_bp.route("/api/operational-summary")
@login_required
def api_operational_summary():
    """Operational summary for managers and viewers (location-filtered)"""
    # This currently reuses sales_summary but will be filtered by location
    return api_sales_summary_combined()
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
    """Get available filters (clients, locations, sites) with associations"""
    try:
        # Get all clients
        clients = [c.client_name for c in DimClient.query.with_entities(DimClient.client_name).distinct().order_by(DimClient.client_name).all()]
        
        # Get associations between locations, sites, and clients
        # We query FactShift joined with DimJob and DimClient to get real-world associations
        associations_query = db.session.query(
            DimJob.location, 
            DimJob.site,
            DimClient.client_name
        ).join(
            FactShift, FactShift.job_id == DimJob.job_id
        ).join(
            DimClient, FactShift.client_id == DimClient.client_id
        )
        
        # Apply role-based filtering
        associations_query = apply_dashboard_filters(associations_query)
        associations = associations_query.distinct().all()
        
        locations_map = {}
        for loc, site, client in associations:
            if not loc:
                continue
            if loc not in locations_map:
                locations_map[loc] = {"sites": set(), "clients": set()}
            if site:
                locations_map[loc]["sites"].add(site)
            # Filter clients as well? For now, we just map what's visible
            if client:
                locations_map[loc]["clients"].add(client)
        
        # Add locations that might not have data yet but exist in DimJob
        all_jobs_query = db.session.query(DimJob.location, DimJob.site)
        all_jobs_query = apply_dashboard_filters(all_jobs_query)
        all_jobs = all_jobs_query.distinct().all()
        
        for loc, site in all_jobs:
            if not loc:
                continue
            if loc not in locations_map:
                locations_map[loc] = {"sites": set(), "clients": set()}
            if site:
                locations_map[loc]["sites"].add(site)

        locations_data = []
        for loc in sorted(locations_map.keys()):
            locations_data.append({
                "name": loc,
                "sites": sorted(list(locations_map[loc]["sites"])),
                "clients": sorted(list(locations_map[loc]["clients"]))
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
        
        if not start or not end or not dimension or not metrics:
            return jsonify({"error": "Missing required parameters"}), 400

        # Security check: Non-admins cannot access financial metrics
        if current_user.role != 'admin':
            financial_metrics = {'revenue', 'cost', 'profit', 'profit_margin', 'avg_bill_rate', 'avg_pay_rate'}
            if any(m in financial_metrics for m in metrics):
                return jsonify({"error": "Access denied: Financial metrics are restricted to administrators."}), 403
            
            # Verify user has access to at least one location
            try:
                import json
                user_locations = json.loads(current_user.location) if current_user.location else []
                if not user_locations:
                    return jsonify({"error": "No locations assigned. Please contact an administrator."}), 403
            except json.JSONDecodeError:
                return jsonify({"error": "Invalid location data. Please contact an administrator."}), 403

        # Map dimension to model column
        dim_map = {
            "date": func.to_char(func.cast(DimDate.date, db.Date), 'YYYY-MM-DD'),
            "month": func.to_char(func.cast(DimDate.date, db.Date), 'Mon YYYY'),
            "year": func.cast(DimDate.year, db.String),
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

        split_by_location = request.args.get("split_by_location", "").lower() == "true"
        split_by_site = request.args.get("split_by_site", "").lower() == "true"

        dim_col = dim_map[dimension]
        metric_cols = [metric_map[m].label(m) for m in valid_metrics]

        query_cols = [dim_col.label("name")]
        if split_by_location:
            query_cols.append(DimJob.location.label("location"))
        if split_by_site:
            query_cols.append(DimJob.site.label("site"))
        
        query_cols.extend(metric_cols)

        query = db.session.query(*query_cols).join(
            DimDate, FactShift.date_id == DimDate.date_id
        )

        # Join other tables if needed for dimensions
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
        elif dimension in ["job_name", "location", "site"]: # Added "site" for completeness, though DimJob.site is accessed via DimJob
            ensure_join(DimJob, FactShift.job_id == DimJob.job_id)
            
        # Ensure DimJob join if splitting by location or site
        if split_by_location or split_by_site:
            ensure_join(DimJob, FactShift.job_id == DimJob.job_id)
            
        query = query.filter(
            DimDate.date >= start,
            DimDate.date <= end
        )

        # Apply global role-based location filtering and requested filters
        query = apply_dashboard_filters(query)

        group_by_cols = [dim_col]
        order_by_cols = [dim_col]

        if split_by_location:
            group_by_cols.append(DimJob.location)
            order_by_cols.append(DimJob.location)
        if split_by_site:
            group_by_cols.append(DimJob.site)
            order_by_cols.append(DimJob.site)
            
        query = query.group_by(*group_by_cols)
        
        # Order by dimension
        if dimension == "month":
            query = query.order_by(func.min(DimDate.date))
        else:
            query = query.order_by(*order_by_cols)

        results = query.all()

        # NEW: Calculate Top 3 Clients for the current filters
        top_clients_query = db.session.query(
            DimClient.client_name,
            func.sum(FactShift.client_net).label("revenue")
        ).join(
            FactShift, FactShift.client_id == DimClient.client_id
        ).join(
            DimDate, FactShift.date_id == DimDate.date_id
        )

        # Apply same filters
        top_clients_query = top_clients_query.filter(
            DimDate.date >= start,
            DimDate.date <= end
        )
        top_clients_query = apply_dashboard_filters(top_clients_query)
        
        top_clients_results = top_clients_query.group_by(
            DimClient.client_name
        ).order_by(
            desc("revenue")
        ).limit(3).all()

        top_clients_data = [
            {"name": name, "revenue": round(float(rev or 0), 2)}
            for name, rev in top_clients_results
        ]

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
            if split_by_location:
                item["location"] = row.location
            if split_by_site:
                item["site"] = row.site
            
            for m in valid_metrics:
                val = getattr(row, m)
                item[m] = round(float(val or 0), 2)
            data.append(item)

        return jsonify({
            "data": data,
            "summary": {
                "topClients": top_clients_data
            }
        })

    except Exception as e:
        current_app.logger.error(f"Error in chart-data API: {e}")
        return jsonify({"error": str(e)}), 500