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
from sqlalchemy import func, case, desc, or_

from . import db
from .models import FactShift, DimClient, DimDate, FinancialMetric
from .auth import admin_required, manager_required
from .models import FactShift, DimEmployee, DimClient, DimJob, DimDate, DimShift
from .utils.data_loader import dbDataLoader

main_bp = Blueprint("main", __name__)

# ... (existing imports and strict column definitions remain unchanged) ...

def get_overheads(start_date, end_date, locations=None, sites=None):
    """Calculate total overheads for a given date range and filters"""
    try:
        # Get unique year/month combinations in the range
        dates = db.session.query(DimDate.year, DimDate.month)\
            .filter(DimDate.date >= start_date.strftime('%Y-%m-%d'))\
            .filter(DimDate.date <= end_date.strftime('%Y-%m-%d'))\
            .distinct().all()
        
        if not dates:
            return 0.0

        total_overheads = 0.0
        
        for year, month in dates:
            query = db.session.query(func.sum(FinancialMetric.value))\
                .filter(FinancialMetric.year == year)\
                .filter(FinancialMetric.month == month)
            
            if locations:
                query = query.filter(FinancialMetric.location.in_(locations))
            if sites:
                query = query.filter(FinancialMetric.site.in_(sites))
                
            monthly_total = query.scalar() or 0.0
            total_overheads += monthly_total
            
        return float(total_overheads)
    except Exception as e:
        current_app.logger.error(f"Error calculating overheads: {e}")
        return 0.0

@main_bp.route("/api/dashboard/breakdown")
@login_required
def api_dashboard_breakdown():
    """Get granular breakdown of metrics by dimension (location, site, client)"""
    try:
        metric = request.args.get("metric", "revenue")
        dimension = request.args.get("dimension", "location")
        start = request.args.get("start")
        end = request.args.get("end")
        limit = int(request.args.get("limit", 20))
        
        if not start or not end:
            return jsonify({"error": "Start and end dates are required"}), 400

        # Base query setup
        if dimension == 'location':
            group_col = DimJob.location
            name_col = DimJob.location
        elif dimension == 'site':
            # Fallback to location if site is null/empty
            # We use coalesce. Note: Empty string might need handling if not null.
            # Assuming empty strings are filtered or treated as null in DB, or we use nullif.
            # For robustness: func.coalesce(func.nullif(DimJob.site, ''), DimJob.location)
            site_expr = func.coalesce(func.nullif(DimJob.site, ''), DimJob.location)
            group_col = site_expr
            name_col = site_expr
        elif dimension == 'client':
            group_col = DimClient.client_name
            name_col = DimClient.client_name
        else:
            return jsonify({"error": "Invalid dimension"}), 400

        # Metric aggregation logic
        if metric == 'revenue':
            value_col = func.sum(FactShift.client_net)
        elif metric == 'cost':
            value_col = func.sum(FactShift.total_pay)
        elif metric == 'profit':
            value_col = func.sum(FactShift.client_net - FactShift.total_pay)
        elif metric == 'hours':
            value_col = func.sum(FactShift.paid_hours)
        elif metric == 'shifts':
            value_col = func.count(FactShift.shift_record_id)
        elif metric == 'clientRate':
            value_col = func.sum(FactShift.client_net) / func.nullif(func.sum(FactShift.paid_hours), 0)
        elif metric == 'staffRate':
            value_col = func.sum(FactShift.total_pay) / func.nullif(func.sum(FactShift.paid_hours), 0)
        elif metric == 'overheads':
             # Special case for Overheads - different table
             # Aggregating FinancialMetric by location/site
             # Client dimension not applicable for overheads (unless mapped?)
             if dimension not in ['location', 'site']:
                 return jsonify([])
             
             start_date = datetime.strptime(start, '%Y-%m-%d')
             end_date = datetime.strptime(end, '%Y-%m-%d')
             
             dates = db.session.query(DimDate.year, DimDate.month)\
                .filter(DimDate.date >= start, DimDate.date <= end)\
                .distinct().all()
                
             results = []
             # This is tricky because FinancialMetric is by Month, not day.
             # We aggregate for the months included.
             # To show breakdown by location/site:
             if dimension == 'site':
                 site_expr = func.coalesce(func.nullif(FinancialMetric.site, ''), FinancialMetric.location)
                 query = db.session.query(
                     site_expr.label('name'),
                     func.sum(FinancialMetric.value).label('value')
                 )
                 query = query.group_by(site_expr)
             else:
                 query = db.session.query(
                     getattr(FinancialMetric, dimension).label('name'),
                     func.sum(FinancialMetric.value).label('value')
                 )
                 query = query.group_by(getattr(FinancialMetric, dimension))
             
             # Filter by years/months in range
             date_filters = [
                 (FinancialMetric.year == d.year) & (FinancialMetric.month == d.month)
                 for d in dates
             ]
             if date_filters:
                 query = query.filter(or_(*date_filters))
             else:
                 return jsonify([]) # No dates match
                 
             query = query.group_by(getattr(FinancialMetric, dimension))
             query = query.order_by(desc('value')).limit(limit)
             
             data = query.all()
             return jsonify([{"name": r[0] or "Unknown", "value": float(r[1] or 0), "count": 0} for r in data])

        else:
            return jsonify({"error": "Invalid metric"}), 400

        # Query construction for standard metrics
        query = db.session.query(name_col, value_col.label('value'))\
            .join(DimDate, FactShift.date_id == DimDate.date_id)
            
        if dimension in ['location', 'site']:
             query = query.join(DimJob, FactShift.job_id == DimJob.job_id)
        if dimension == 'client':
             query = query.join(DimClient, FactShift.client_id == DimClient.client_id)
             
        query = query.filter(DimDate.date >= start, DimDate.date <= end)
        
        # Apply role-based filters
        query = apply_dashboard_filters(query)
        
        results = query.group_by(group_col)\
            .order_by(desc('value'))\
            .limit(limit).all()
            
        return jsonify([
            {"name": r[0] or "Unknown", "value": float(r[1] or 0), "count": 0} 
            for r in results
        ])

    except Exception as e:
        current_app.logger.error(f"Error in breakdown API: {e}")
        return jsonify([]), 500

@main_bp.route("/api/financial-metrics/list")
@login_required
def api_financial_metrics_list():
    """Get list of financial metrics (overheads)"""
    try:
        start = request.args.get("start")
        end = request.args.get("end")
        requested_locations = request.args.getlist("locations")
        requested_sites = request.args.getlist("sites")
        
        if not start or not end:
            return jsonify([]), 400
            
        start_date = datetime.strptime(start, '%Y-%m-%d')
        end_date = datetime.strptime(end, '%Y-%m-%d')
        
        # Get unique year/month combinations in the range
        dates = db.session.query(DimDate.year, DimDate.month)\
            .filter(DimDate.date >= start, DimDate.date <= end)\
            .distinct().all()
            
        if not dates:
             return jsonify([])

        query = db.session.query(FinancialMetric)
        
        # Filter by dates (year/month tuples)
        date_filters = [
             (FinancialMetric.year == d.year) & (FinancialMetric.month == d.month)
             for d in dates
        ]
        if date_filters:
            query = query.filter(or_(*date_filters))
        else:
            return jsonify([])
            
        if requested_locations:
            query = query.filter(FinancialMetric.location.in_(requested_locations))
        if requested_sites:
            query = query.filter(FinancialMetric.site.in_(requested_sites))
            
        metrics = query.order_by(FinancialMetric.year.desc(), FinancialMetric.month.desc(), FinancialMetric.name).all()
        
        return jsonify([{
            "id": m.id,
            "year": m.year,
            "month": m.month,
            "name": m.name,
            "value": float(m.value),
            "location": m.location,
            "site": m.site
        } for m in metrics])
        
    except Exception as e:
        current_app.logger.error(f"Error fetching financial metrics list: {e}")
        return jsonify([]), 500

@main_bp.route("/api/sales-summary")
@login_required
def api_sales_summary_combined():
    """Combined endpoint that calls both totals and timeseries"""
    try:
        start = request.args.get("start")
        end = request.args.get("end")
        requested_locations = request.args.getlist("locations")
        requested_sites = request.args.getlist("sites")
        
        if not start or not end:
            return jsonify({"error": "Start and end dates are required"}), 400
        
        # Get totals data directly (no ThreadPoolExecutor)
        from sqlalchemy import distinct
        from datetime import datetime, timedelta
        
        start_date = datetime.strptime(start, '%Y-%m-%d')
        end_date = datetime.strptime(end, '%Y-%m-%d')
        days_diff = (end_date - start_date).days
        
        # Calculate Overheads
        current_overheads = get_overheads(start_date, end_date, requested_locations, requested_sites)
        
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
        
        previous_overheads = get_overheads(previous_start, previous_end, requested_locations, requested_sites)
        
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
             current_overheads = 0.0 # Strict sanitization
             
             # Extract operational data
             current_paid_hours = float(current_totals[5] or 0)
             
             previous_revenue = 0.0
             previous_cost = 0.0
             previous_profit_margin = 0.0
             previous_avg_client_pay = 0.0
             previous_avg_staff_pay = 0.0
             previous_overheads = 0.0 # Strict sanitization
             previous_paid_hours = float(previous_totals[5] or 0)
        else:
             # Calculate metrics for current period
             current_revenue = float(current_totals[0] or 0)
             current_cost = float(current_totals[1] or 0)
             current_paid_hours = float(current_totals[5] or 0)
             
             # Add overheads to cost? Or separate metric?
             # User requested "Total Overheads" separately.
             # Profit calculation should technically include overheads: Revenue - (Cost + Overheads)
             # Let's update profit margin formula to include overheads for accuracy
             total_expenses = current_cost + current_overheads
             current_profit_margin = ((current_revenue - total_expenses) / current_revenue * 100) if current_revenue > 0 else 0
             
             current_avg_client_pay = (current_revenue / current_paid_hours) if current_paid_hours > 0 else 0
             current_avg_staff_pay = (current_cost / current_paid_hours) if current_paid_hours > 0 else 0
             
             # Calculate metrics for previous period
             previous_revenue = float(previous_totals[0] or 0)
             previous_cost = float(previous_totals[1] or 0)
             previous_paid_hours = float(previous_totals[5] or 0)
             
             prev_total_expenses = previous_cost + previous_overheads
             previous_profit_margin = ((previous_revenue - prev_total_expenses) / previous_revenue * 100) if previous_revenue > 0 else 0
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
        
        # Fetch overheads for time series injection
        # Note: FinancialMetric uses full month name (e.g. "January")
        full_month_map = {
             "January": "01", "February": "02", "March": "03", "April": "04",
             "May": "05", "June": "06", "July": "07", "August": "08",
             "September": "09", "October": "10", "November": "11", "December": "12"
        }
        
        overheads_query = db.session.query(
             FinancialMetric.year, 
             FinancialMetric.month, 
             func.sum(FinancialMetric.value)
        ).filter(
             FinancialMetric.year >= start_date.year,
             FinancialMetric.year <= end_date.year
        )
        
        if requested_locations:
            overheads_query = overheads_query.filter(FinancialMetric.location.in_(requested_locations))
        if requested_sites:
            overheads_query = overheads_query.filter(FinancialMetric.site.in_(requested_sites))
            
        overheads_results = overheads_query.group_by(FinancialMetric.year, FinancialMetric.month).all()
        
        overheads_map = {}
        for y, m, v in overheads_results:
            m_num = full_month_map.get(m, "00")
            key = f"{y}-{m_num}"
            overheads_map[key] = float(v or 0)

        time_series_data = []
        for period, display, revenue, cost, paid_hours, shifts in time_series_results:
             # Try to match overheads
             # If aggregation is monthly, period is YYYY-MM
             # If aggregation is daily/weekly, we need to derive YYYY-MM from `period` (which might be a date)
             
             ov_val = 0.0
             period_str = str(period)
             
             if aggregation_level == 'monthly':
                 ov_key = period_str # YYYY-MM
                 ov_val = overheads_map.get(ov_key, 0.0)
             else:
                 # Attempt to extract YYYY-MM from date string YYYY-MM-DD
                 if len(period_str) >= 7:
                     ov_key = period_str[:7]
                     if aggregation_level == 'daily':
                         # Pro-rate? Or just show 0?
                         # Showing full monthly overhead on every day is wrong.
                         # Showing it on the 1st of month?
                         if period_str.endswith("-01"):
                              ov_val = overheads_map.get(ov_key, 0.0)
                     elif aggregation_level == 'weekly':
                         # difficult to map perfect weeks to months
                         pass
            
             time_series_data.append({
                "period": str(period),
                "display": display,
                "revenue": round(float(revenue or 0), 2) if current_user.role == 'admin' else 0.0,
                "cost": round(float(cost or 0), 2) if current_user.role == 'admin' else 0.0,
                "paidHours": round(float(paid_hours or 0), 2),
                "totalShifts": int(shifts or 0),
                "overheads": round(ov_val, 2) if current_user.role == 'admin' else 0.0
            })

        if current_user.role == 'admin':
             # We need to inject overheads into time series?
             # That would be complex as overheads are monthly. 
             # For now, let's just return the aggregate totalOverheads.
             pass
        
        # Build final response
        payload = {
            "totalRevenue": round(current_revenue, 2),
            "totalCost": round(current_cost, 2),
            "totalOverheads": round(current_overheads, 2),
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
                "totalOverheads": round(previous_overheads, 2),
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

@main_bp.route("/api/sites")
@login_required
def api_list_sites():
    """Get list of sites, optionally filtered by locations"""
    try:
        requested_locations = request.args.getlist("locations")
        query = db.session.query(DimJob.site).distinct().filter(
            DimJob.site.isnot(None),
            DimJob.site != ''
        )
        
        if requested_locations:
             query = query.filter(DimJob.location.in_(requested_locations))
             
        # RBAC
        query = apply_dashboard_filters(query)
             
        sites = query.order_by(DimJob.site).all()
        return jsonify([s[0] for s in sites])
        
    except Exception as e:
        current_app.logger.error(f"Error fetching sites: {e}")
        return jsonify([]), 500

@main_bp.route("/api/rankings/staff")
@login_required
def api_rankings_staff():
    """Get top staff by various metrics"""
    try:
        start = request.args.get("start")
        end = request.args.get("end")
        limit = int(request.args.get("limit", 10))
        
        if not start or not end:
            return jsonify([]), 400
            
        # Metric: 'hours' (default) or 'cost' (if admin)
        metric = request.args.get("metric", "hours")
        
        query = db.session.query(
            DimEmployee.full_name.label('name'),
            func.sum(FactShift.paid_hours).label('value'),
            func.count(FactShift.shift_record_id).label('shifts')
        ).join(
            FactShift, FactShift.employee_id == DimEmployee.employee_id
        ).join(
            DimDate, FactShift.date_id == DimDate.date_id
        ).filter(
            DimDate.date >= start,
            DimDate.date <= end
        )
        
        # Apply filters
        query = apply_dashboard_filters(query) 
        
        if metric == 'cost' and current_user.role == 'admin':
             # Override query for cost
             query = db.session.query(
                DimEmployee.full_name.label('name'),
                func.sum(FactShift.total_pay).label('value'),
                func.count(FactShift.shift_record_id).label('shifts')
             ).join(
                FactShift, FactShift.employee_id == DimEmployee.employee_id
             ).join(
                DimDate, FactShift.date_id == DimDate.date_id
             ).filter(
                DimDate.date >= start,
                DimDate.date <= end
             )
             query = apply_dashboard_filters(query)

        results = query.group_by(DimEmployee.full_name)\
            .order_by(desc('value'))\
            .limit(limit).all()
            
        return jsonify([{
            "name": r.name,
            "value": float(r.value or 0),
            "subValue": f"{r.shifts} shifts"
        } for r in results])
        
    except Exception as e:
        current_app.logger.error(f"Error in staff rankings: {e}")
        return jsonify([]), 500

@main_bp.route("/api/rankings/clients")
@login_required
def api_rankings_clients():
    """Get top clients by revenue or shifts"""
    try:
        start = request.args.get("start")
        end = request.args.get("end")
        limit = int(request.args.get("limit", 10))
        metric = request.args.get("metric", "revenue") # revenue or shifts
        
        if not start or not end:
            return jsonify([]), 400
            
        if metric == 'revenue':
             if current_user.role != 'admin':
                 return jsonify({"error": "Unauthorized"}), 403
             col = func.sum(FactShift.client_net)
             sort_desc = desc('value')
        else:
             col = func.count(FactShift.shift_record_id)
             sort_desc = desc('value')
             
        query = db.session.query(
            DimClient.client_name.label('name'),
            col.label('value')
        ).join(
            FactShift, FactShift.client_id == DimClient.client_id
        ).join(
            DimDate, FactShift.date_id == DimDate.date_id
        ).filter(
            DimDate.date >= start,
            DimDate.date <= end
        )
        
        query = apply_dashboard_filters(query)
        
        results = query.group_by(DimClient.client_name)\
            .order_by(sort_desc)\
            .limit(limit).all()
            
        return jsonify([{
            "name": r.name,
            "value": float(r.value or 0)
        } for r in results])
        
    except Exception as e:
        current_app.logger.error(f"Error in client rankings: {e}")
        return jsonify([]), 500

@main_bp.route("/api/chart-data")
@login_required
def api_chart_data():
    """Get aggregated chart data based on dimension and metrics"""
    try:
        start = request.args.get("start")
        end = request.args.get("end")
        dimension = request.args.get("dimension", "").lower()
        metrics = [m.lower() for m in request.args.getlist("metrics")]
        split_by_location = request.args.get("split_by_location") == 'true'
        split_by_site = request.args.get("split_by_site") == 'true'
        
        if not start or not end or not dimension or not metrics:
            return jsonify({"error": "Missing required parameters"}), 400

        # Security check: Non-admins cannot access financial metrics
        if current_user.role != 'admin':
            financial_metrics = {'revenue', 'cost', 'profit', 'profit_margin', 'avg_bill_rate', 'avg_pay_rate', 'overheads', 'labor_cost', 'net_profit', 'gross_profit'}
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
            "site": DimJob.site,
        }

        def apply_dashboard_filters(q):
            clients = request.args.getlist("clients")
            locations = request.args.getlist("locations")
            sites = request.args.getlist("sites")
            
            # Helper to check if table is already joined (naive string check for now/simple context)
            # Or always join with explicit ON clause which SQLAlchemy handles (deduplicates if same path? No.)
            # We will use explicit checks based on known query structure or just try/except? 
            # No, let's just join. SQLAlchemy usually errors on duplicate join.
            # But for 'top_clients_query', DimJob is NOT joined. For 'query', it MIGHT be.
            
            # Strategy: Logic to ensure join.
            # Convert query to string to check existence of table name is risky but effective for simple aliases.
            q_str = str(q.statement.compile(compile_kwargs={"literal_binds": True})) if hasattr(q, 'statement') else ""
            
            if clients:
                if "dim_clients" not in q_str and "DimClient" not in str(q): # Naive check
                     try:
                         q = q.join(DimClient, FactShift.client_id == DimClient.client_id)
                     except: pass # Already joined or error
                q = q.filter(DimClient.client_name.in_(clients))
                
            if locations:
                if "dim_jobs" not in q_str and "DimJob" not in str(q):
                     try:
                        q = q.join(DimJob, FactShift.job_id == DimJob.job_id)
                     except: pass
                q = q.filter(DimJob.location.in_(locations))
                
            if sites:
                if "dim_jobs" not in q_str and "DimJob" not in str(q): # check again
                     try:
                        q = q.join(DimJob, FactShift.job_id == DimJob.job_id)
                     except: pass
                q = q.filter(DimJob.site.in_(sites))
                
            return q


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
            
        dim_col = dim_map[dimension]
        
        # Validate metrics
        valid_metrics = []
        for m in metrics:
            if m in metric_map:
                valid_metrics.append(m)
        
        if not valid_metrics and not dimension in ['month', 'year']: 
             # Allow empty valid_metrics if we are querying financial metrics by time? 
             # No, let's keep it simple. If valid_metrics is empty, check if we have financial metrics requests.
             pass

        # Check for financial metrics requests (anything not in standard map)
        requested_financials = [m for m in metrics if m not in metric_map]
        
        # Special case: If ONLY financial metrics requested (no standard metrics)
        # We skip the FactShift query entirely and build results from FinancialMetric table
        if not valid_metrics and requested_financials and dimension in ['month', 'year'] and not split_by_location and not split_by_site:
            from .models import FinancialMetric
            from datetime import datetime
            
            start_year = int(start[:4])
            end_year = int(end[:4])
            
            # Build time periods from date range
            start_date = datetime.strptime(start, '%Y-%m-%d')
            end_date = datetime.strptime(end, '%Y-%m-%d')
            
            # Get distinct periods from DimDate
            if dimension == 'month':
                periods_query = db.session.query(
                    func.to_char(func.cast(DimDate.date, db.Date), 'Mon YYYY').label('period'),
                    func.min(DimDate.date).label('min_date')
                ).filter(
                    DimDate.date >= start,
                    DimDate.date <= end
                ).group_by(func.to_char(func.cast(DimDate.date, db.Date), 'Mon YYYY')).order_by('min_date')
            else:  # year
                periods_query = db.session.query(
                    func.cast(DimDate.year, db.String).label('period')
                ).filter(
                    DimDate.date >= start,
                    DimDate.date <= end
                ).distinct().order_by(DimDate.year)
            
            periods = periods_query.all()
            
            # Fetch financial data
            financial_data_map = {}
            if 'overheads' in requested_financials:
                fin_aggregates = db.session.query(
                    FinancialMetric.year,
                    FinancialMetric.month,
                    func.sum(FinancialMetric.value).label('total')
                ).filter(
                    FinancialMetric.year >= start_year,
                    FinancialMetric.year <= end_year
                ).group_by(
                    FinancialMetric.year,
                    FinancialMetric.month
                ).all()
                
                short_map = {
                    "January": "Jan", "February": "Feb", "March": "Mar", "April": "Apr", 
                    "May": "May", "June": "Jun", "July": "Jul", "August": "Aug", 
                    "September": "Sep", "October": "Oct", "November": "Nov", "December": "Dec"
                }
                
                for agg in fin_aggregates:
                    if dimension == 'month':
                        key = f"{short_map.get(agg.month, agg.month)} {agg.year}"
                    else:
                        key = str(agg.year)
                    financial_data_map[key] = {'overheads': float(agg.total or 0)}
            
            # Build results
            data = []
            for period_row in periods:
                period_name = period_row[0] if isinstance(period_row, tuple) else period_row.period
                item = {"name": period_name}
                
                # Add financial metrics
                if period_name in financial_data_map:
                    for fm_name in requested_financials:
                        item[fm_name] = financial_data_map[period_name].get(fm_name, 0)
                else:
                    for fm_name in requested_financials:
                        item[fm_name] = 0
                
                data.append(item)
            
            return jsonify({
                "data": data,
                "summary": {
                    "topClients": []
                }
            })
        
        # ... logic continues ...
        
        metric_cols = [metric_map[m].label(m) for m in valid_metrics]

        query_cols = [dim_col.label("name")]
        if split_by_location:
            query_cols.append(DimJob.location.label("location"))
        if split_by_site:
            query_cols.append(DimJob.site.label("site"))
        
        query_cols.extend(metric_cols)

        # Execute Main Query
        query = db.session.query(*query_cols).select_from(FactShift).join(
            DimDate, FactShift.date_id == DimDate.date_id
        )
        
        # ... (Join logic same as before) ...
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
        elif dimension in ["job_name", "location", "site"]: 
            ensure_join(DimJob, FactShift.job_id == DimJob.job_id)
            
        # Ensure DimJob join if splitting by location or site
        if split_by_location or split_by_site:
            ensure_join(DimJob, FactShift.job_id == DimJob.job_id)
            
        query = query.filter(
            DimDate.date >= start,
            DimDate.date <= end
        )

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
        
        if dimension == "month":
            query = query.order_by(func.min(DimDate.date))
        else:
            query = query.order_by(*order_by_cols)

        results = query.all()

        # FETCH FINANCIAL DATA IF NEEDED
        financial_data_map = {} # Key: "Month Year" or "Year" -> { metric: value }
        
        if requested_financials and (dimension == 'month' or dimension == 'year') and not split_by_location and not split_by_site:
            from .models import FinancialMetric
            # Parse start/end year
            start_year = int(start[:4])
            end_year = int(end[:4])
            
            # Aggregate ALL financial metrics as "overheads" (sum across all names)
            if 'overheads' in requested_financials:
                # Group by year and month, SUM all values
                fin_aggregates = db.session.query(
                    FinancialMetric.year,
                    FinancialMetric.month,
                    func.sum(FinancialMetric.value).label('total')
                ).filter(
                    FinancialMetric.year >= start_year,
                    FinancialMetric.year <= end_year
                ).group_by(
                    FinancialMetric.year,
                    FinancialMetric.month
                ).all()
                
                for agg in fin_aggregates:
                    # Key format must match "Mon YYYY" or "YYYY"
                    if dimension == 'month':
                        # FinancialMetric.month is Full Name (January), we need Short Name (Jan)
                        short_map = {
                            "January": "Jan", "February": "Feb", "March": "Mar", "April": "Apr", 
                            "May": "May", "June": "Jun", "July": "Jul", "August": "Aug", 
                            "September": "Sep", "October": "Oct", "November": "Nov", "December": "Dec"
                        }
                        key = f"{short_map.get(agg.month, agg.month)} {agg.year}"
                    else: 
                        key = str(agg.year)
                    
                    if key not in financial_data_map:
                        financial_data_map[key] = {}
                    financial_data_map[key]['overheads'] = float(agg.total or 0)

        # NEW: Calculate Top 3 Clients (unchanged...)
        top_clients_query = db.session.query(
            DimClient.client_name,
            func.sum(FactShift.client_net).label("revenue")
        ).join(
            FactShift, FactShift.client_id == DimClient.client_id
        ).join(
            DimDate, FactShift.date_id == DimDate.date_id
        )

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
            name_value = str(row.name)
            # Standard Abbrev
            if dimension == "month" and name_value in month_abbrev:
                name_value = month_abbrev[name_value]
            
            item = {"name": name_value}
            
            if split_by_location:
                item["location"] = row.location
            if split_by_site:
                item["site"] = row.site
            
            # Add Standard Metrics
            for m in valid_metrics:
                val = getattr(row, m)
                item[m] = round(float(val or 0), 2)
            
            # Add Financial Metrics (merged)
            if requested_financials and name_value in financial_data_map:
                 for fm_name in requested_financials:
                     if fm_name in financial_data_map[name_value]:
                         item[fm_name] = financial_data_map[name_value][fm_name]
                     else:
                         item[fm_name] = 0

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

@main_bp.route("/api/dashboard/client-distribution")
@login_required
def api_client_distribution():
    """
    Get distinct client counts over time (monthly).
    """
    try:
        start = request.args.get("start")
        end = request.args.get("end")
        
        if not start or not end:
            return jsonify({"error": "Missing start/end date parameters"}), 400
            
        # Re-implement filters logic (similar to api_chart_data)
        clients = request.args.getlist("clients")
        locations = request.args.getlist("locations")
        sites = request.args.getlist("sites")
        
        # Base Query
        query = db.session.query(
            func.to_char(func.cast(DimDate.date, db.Date), 'Mon YYYY').label('period'),
            DimDate.year,
            DimDate.month,
            func.count(func.distinct(FactShift.client_id)).label('client_count')
        ).select_from(FactShift).join(
            DimDate, FactShift.date_id == DimDate.date_id
        )
        
        # Joins for filters
        if locations or sites:
            query = query.join(DimJob, FactShift.job_id == DimJob.job_id)
        if clients:
            query = query.join(DimClient, FactShift.client_id == DimClient.client_id)
            
        # Apply Filters
        query = query.filter(
            DimDate.date >= start,
            DimDate.date <= end
        )
        
        if clients:
            query = query.filter(DimClient.client_name.in_(clients))
        if locations:
            query = query.filter(DimJob.location.in_(locations))
        if sites:
            query = query.filter(DimJob.site.in_(sites))
            
        # RBAC Check
        if current_user.role != 'admin':
             # Ensure we join DimJob if not already joined
             if not locations and not sites: # If we haven't joined yet
                 query = query.join(DimJob, FactShift.job_id == DimJob.job_id)
             
             try:
                import json
                user_locations = json.loads(current_user.location) if current_user.location else []
                if user_locations:
                    query = query.filter(DimJob.location.in_(user_locations))
                else:
                    # No locations = no access? Or check string
                    query = query.filter(DimJob.location == current_user.location)
             except:
                query = query.filter(DimJob.location == current_user.location)

        # Group and Order
        query = query.group_by(
            func.to_char(func.cast(DimDate.date, db.Date), 'Mon YYYY'),
            DimDate.year,
            DimDate.month
        ).order_by(
            func.min(DimDate.date)
        )
        
        results = query.all()
        
        data = []
        for r in results:
            data.append({
                "period": r.period,
                "display": r.period,
                "clientCount": r.client_count
            })
            
        return jsonify({
            "timeSeries": data,
            "aggregationLevel": "monthly"
        })

    except Exception as e:
        current_app.logger.error(f"Error in client-distribution API: {e}")
        return jsonify({"error": str(e)}), 500