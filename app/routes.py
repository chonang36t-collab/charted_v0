"""Main routes blueprint: dashboard, upload, and API for analysis."""
from __future__ import annotations
from sqlalchemy import text
from app import cache

import io
from datetime import datetime, timedelta
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
from .models import FactShift, DimEmployee, DimClient, DimJob, DimDate, DimShift, ShiftTarget
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

        if metric == 'targetAchievement':
            if dimension == 'client':
                return jsonify([])

            # Convert string dates if needed
            try:
                # Ensure they are YYYY-MM-DD for consistency
                datetime.strptime(start, '%Y-%m-%d')
                datetime.strptime(end, '%Y-%m-%d')
            except:
                pass # Use as is if valid or let query fail gracefully

            # 1. Actuals
            group_col = DimJob.site if dimension == 'site' else DimJob.location
            actuals_query = db.session.query(
                group_col.label('name'),
                func.count(FactShift.shift_record_id).label('val')
            ).join(DimJob, FactShift.job_id == DimJob.job_id)\
             .join(DimDate, FactShift.date_id == DimDate.date_id)\
             .filter(DimDate.date >= start, DimDate.date <= end)

            # RBAC for Actuals
            if current_user.role != 'admin':
                if current_user.location:
                    actuals_query = actuals_query.filter(DimJob.location == current_user.location)
                if current_user.site:
                    actuals_query = actuals_query.filter(DimJob.site == current_user.site)
            
            actuals = actuals_query.group_by(group_col).all()
            actual_map = {r.name: r.val for r in actuals if r.name}

            # 2. Targets
            dates_q = db.session.query(DimDate.year, DimDate.month)\
                .filter(DimDate.date >= start, DimDate.date <= end)\
                .distinct().all()
            
            target_map = {}
            if dates_q:
                for year, month in dates_q:
                    q = ShiftTarget.query.filter_by(year=year, month=month)
                    if current_user.role != 'admin':
                        if current_user.location:
                             q = q.filter_by(location=current_user.location)
                        if current_user.site:
                             q = q.filter_by(site=current_user.site)
                    
                    for t in q.all():
                        key = t.site if dimension == 'site' else t.location
                        if key:
                            target_map[key] = target_map.get(key, 0) + t.target_count

            # 3. Merge
            data = []
            all_keys = set(actual_map.keys()) | set(target_map.keys())
            for k in all_keys:
                act = actual_map.get(k, 0)
                tgt = target_map.get(k, 0)
                if tgt > 0:
                    pct = (act / tgt) * 100
                    data.append({"name": k, "value": round(pct, 1), "actual": act, "target": tgt})
                elif act > 0:
                    data.append({"name": k, "value": 100.0, "actual": act, "target": 0})
            
            data.sort(key=lambda x: x['value'], reverse=True)
            return jsonify(data[:limit])

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
        elif metric == 'clients':
             # For client metric, behavior depends on dimension:
             # - By location/site: count distinct clients in that location/site
             # - By client: count distinct sites where that client was active
             if dimension == 'client':
                 value_col = func.count(func.distinct(DimJob.site))  # Sites per client
             else:
                 value_col = func.count(func.distinct(FactShift.client_id))  # Clients per location/site
        elif metric == 'employees':
             # Count distinct employees
             value_col = func.count(func.distinct(FactShift.employee_id))
        else:
            return jsonify({"error": "Invalid metric"}), 400

        # Query construction for standard metrics
        query = db.session.query(name_col, value_col.label('value'))\
            .join(DimDate, FactShift.date_id == DimDate.date_id)
        
        # Filter out NULL client_ids for client counting
        if metric == 'clients':
            query = query.filter(FactShift.client_id.isnot(None))
            # Need DimJob join when counting sites per client
            if dimension == 'client':
                query = query.join(DimJob, FactShift.job_id == DimJob.job_id, isouter=False)
            
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
def api_financial_summary():
    """Financial summary with RBAC filtering for non-admin users"""
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
        
        # Base Query - IMPORTANT: Filter out NULL client_ids to get actual client count
        query = db.session.query(
            func.to_char(func.cast(DimDate.date, db.Date), 'YYYY-MM').label('period'),
            func.to_char(func.cast(DimDate.date, db.Date), 'Mon YYYY').label('display'),
            DimDate.year,
            DimDate.month,
            func.count(func.distinct(FactShift.client_id)).label('client_count')
        ).select_from(FactShift).join(
            DimDate, FactShift.date_id == DimDate.date_id
        ).filter(
            FactShift.client_id.isnot(None)  # CRITICAL: Exclude NULL client_ids
        )
        
        # Joins for filters
        job_joined = False
        if locations or sites:
            query = query.join(DimJob, FactShift.job_id == DimJob.job_id)
            job_joined = True
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
             if not job_joined:
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
            func.to_char(func.cast(DimDate.date, db.Date), 'YYYY-MM'),
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
                "period": r.period,     # YYYY-MM format for matching with operational data
                "display": r.display,    # Mon YYYY format for display
                "clientCount": r.client_count
            })
        
        # Return empty array instead of error if no data
        return jsonify({
            "timeSeries": data,
            "aggregationLevel": "monthly"
        })

    except Exception as e:
        current_app.logger.error(f"Error in client-distribution API: {e}")
        return jsonify({"error": str(e)}), 500

@main_bp.route("/api/dashboard/client-revenue-tiers")
@login_required
def api_client_revenue_tiers():
    """
    Segment clients into revenue tiers (Top 20%, Mid-tier, Low-value)
    based on total revenue contribution within the date range.
    ADMIN ONLY - Contains sensitive revenue data.
    """
    # RESTRICT TO ADMIN ONLY
    if current_user.role != 'admin':
        return jsonify({"error": "Admin access required"}), 403
    
    try:
        start = request.args.get("start")
        end = request.args.get("end")
        
        if not start or not end:
            return jsonify({"error": "Missing start/end date parameters"}), 400
        
        # Get filters
        locations = request.args.getlist("locations")
        sites = request.args.getlist("sites")
        
        # Step 1: Calculate revenue per client
        base_query = db.session.query(
            DimClient.client_name.label('client_name'),
            func.sum(FactShift.client_net).label('total_revenue')
        ).select_from(FactShift)\
        .join(DimDate, FactShift.date_id == DimDate.date_id)\
        .join(DimClient, FactShift.client_id == DimClient.client_id)\
        .filter(
            FactShift.client_id.isnot(None),
            FactShift.client_net.isnot(None),
            DimDate.date >= start,
            DimDate.date <= end
        )
        
        # Apply location/site filters
        if locations or sites:
            base_query = base_query.join(DimJob, FactShift.job_id == DimJob.job_id)
            if locations:
                base_query = base_query.filter(DimJob.location.in_(locations))
            if sites:
                base_query = base_query.filter(DimJob.site.in_(sites))
        
        # RBAC: Filter by user's locations if not admin
        if current_user.role != 'admin':
            if not (locations or sites):
                # Join DimJob if not already joined
                base_query = base_query.join(DimJob, FactShift.job_id == DimJob.job_id)
            
            try:
                import json
                user_locations = json.loads(current_user.location) if current_user.location else []
                if user_locations:
                    base_query = base_query.filter(DimJob.location.in_(user_locations))
            except:
                pass
        
        # Group by client
        base_query = base_query.group_by(DimClient.client_name)
        client_revenues = base_query.all()
        
        if not client_revenues:
            return jsonify({"tiers": []})
        
        # Step 2: Calculate percentiles using Python (SQLAlchemy's percentile_cont is complex)
        revenues = [float(r.total_revenue) for r in client_revenues]
        revenues.sort()
        
        # Calculate 50th and 80th percentiles
        def percentile(data, p):
            n = len(data)
            if n == 0:
                return 0
            k = (n - 1) * p
            f = int(k)
            c = k - f
            if f + 1 < n:
                return data[f] + c * (data[f + 1] - data[f])
            else:
                return data[f]
        
        p50 = percentile(revenues, 0.5)
        p80 = percentile(revenues, 0.8)
        
        # Step 3: Segment clients into tiers
        tiers = {
            'Top 20%': {'count': 0, 'total': 0.0, 'clients': []},
            'Mid-tier': {'count': 0, 'total': 0.0, 'clients': []},
            'Low-value': {'count': 0, 'total': 0.0, 'clients': []}
        }
        
        for client_rev in client_revenues:
            revenue = float(client_rev.total_revenue)
            if revenue >= p80:
                tier = 'Top 20%'
            elif revenue >= p50:
                tier = 'Mid-tier'
            else:
                tier = 'Low-value'
            
            tiers[tier]['count'] += 1
            tiers[tier]['total'] += revenue
            tiers[tier]['clients'].append({
                'name': client_rev.client_name,
                'revenue': round(revenue, 2)
            })
        
        # Step 4: Format response
        result = []
        tier_order = ['Top 20%', 'Mid-tier', 'Low-value']
        for tier_name in tier_order:
            tier_data = tiers[tier_name]
            avg_revenue = tier_data['total'] / tier_data['count'] if tier_data['count'] > 0 else 0
            
            # Sort clients by revenue descending within each tier
            sorted_clients = sorted(tier_data['clients'], key=lambda x: x['revenue'], reverse=True)
            
            result.append({
                "tier": tier_name,
                "clientCount": tier_data['count'],
                "totalRevenue": round(tier_data['total'], 2),
                "avgRevenue": round(avg_revenue, 2),
                "clients": sorted_clients
            })
        
        return jsonify({"tiers": result})
    
    except Exception as e:
        current_app.logger.error(f"Error in client-revenue-tiers API: {e}")
        return jsonify({"error": str(e)}), 500

@main_bp.route("/api/dashboard/client-workload-scatter")
@login_required
def api_client_workload_scatter():
    """
    Get client workload vs staff coverage data for scatter chart.
    Returns total hours, distinct staff count, and shift count per client with medians.
    """
    try:
        start = request.args.get("start")
        end = request.args.get("end")
        
        if not start or not end:
            return jsonify({"error": "Missing start/end date parameters"}), 400
        
        # Get filters
        locations = request.args.getlist("locations")
        sites = request.args.getlist("sites")
        
        # Query: Aggregate by client
        base_query = db.session.query(
            DimClient.client_name.label('client_name'),
            func.sum(FactShift.paid_hours).label('total_hours'),
            func.count(func.distinct(FactShift.employee_id)).label('staff_count'),
            func.count(FactShift.shift_record_id).label('shift_count')
        ).select_from(FactShift)\
        .join(DimDate, FactShift.date_id == DimDate.date_id)\
        .join(DimClient, FactShift.client_id == DimClient.client_id)\
        .filter(
            FactShift.client_id.isnot(None),
            DimDate.date >= start,
            DimDate.date <= end
        )
        
        # Apply location/site filters
        if locations or sites:
            base_query = base_query.join(DimJob, FactShift.job_id == DimJob.job_id)
            if locations:
                base_query = base_query.filter(DimJob.location.in_(locations))
            if sites:
                base_query = base_query.filter(DimJob.site.in_(sites))
        
        # RBAC: Filter by user's locations if not admin
        if current_user.role != 'admin':
            if not (locations or sites):
                # Join DimJob if not already joined
                base_query = base_query.join(DimJob, FactShift.job_id == DimJob.job_id)
            
            try:
                import json
                user_locations = json.loads(current_user.location) if current_user.location else []
                if user_locations:
                    base_query = base_query.filter(DimJob.location.in_(user_locations))
            except:
                pass
        
        # Group by client
        base_query = base_query.group_by(DimClient.client_name)
        results = base_query.all()
        
        if not results:
            return jsonify({"data": [], "medians": {"hours": 0, "staffCount": 0}})
        
        # Calculate medians
        hours_list = sorted([float(r.total_hours) for r in results])
        staff_list = sorted([int(r.staff_count) for r in results])
        
        def percentile(data, p):
            n = len(data)
            if n == 0:
                return 0
            k = (n - 1) * p
            f = int(k)
            c = k - f
            if f + 1 < n:
                return data[f] + c * (data[f + 1] - data[f])
            else:
                return data[f]
        
        median_hours = percentile(hours_list, 0.5)
        median_staff = percentile(staff_list, 0.5)
        
        # Format response
        data = []
        for r in results:
            data.append({
                "clientName": r.client_name,
                "totalHours": round(float(r.total_hours), 2),
                "staffCount": int(r.staff_count),
                "shiftCount": int(r.shift_count)
            })
        
        return jsonify({
            "data": data,
            "medians": {
                "hours": round(median_hours, 2),
                "staffCount": round(median_staff, 1)
            }
        })
    
    except Exception as e:
        current_app.logger.error(f"Error in client-workload-scatter API: {e}")
        return jsonify({"error": str(e)}), 500

@main_bp.route("/api/dashboard/shifts-heatmap")
@login_required
def api_shifts_heatmap():
    """
    Get shift density heatmap data.
    Supports two views: calendar (day-by-day) and location_day (location × day grid).
    """
    try:
        start = request.args.get("start")
        end = request.args.get("end")
        view_type = request.args.get("view_type", "calendar")
        
        if not start or not end:
            return jsonify({"error": "Missing start/end date parameters"}), 400
        
        # Get filters
        locations = request.args.getlist("locations")
        sites = request.args.getlist("sites")
        
        if view_type == "location_day":
            # Location × Day heatmap
            query = db.session.query(
                DimDate.date,
                DimJob.location,
                func.count(FactShift.shift_record_id).label('shift_count')
            ).select_from(FactShift)\
            .join(DimDate, FactShift.date_id == DimDate.date_id)\
            .join(DimJob, FactShift.job_id == DimJob.job_id)\
            .filter(
                DimDate.date >= start,
                DimDate.date <= end
            )
            
            # Apply filters
            if locations:
                query = query.filter(DimJob.location.in_(locations))
            if sites:
                query = query.filter(DimJob.site.in_(sites))
            
            # RBAC: Filter by user's locations if not admin
            if current_user.role != 'admin':
                try:
                    import json
                    user_locations = json.loads(current_user.location) if current_user.location else []
                    if user_locations:
                        query = query.filter(DimJob.location.in_(user_locations))
                except Exception as e:
                    current_app.logger.error(f"RBAC error in shifts-heatmap: {e}")
            
            query = query.group_by(DimDate.date, DimJob.location).order_by(DimDate.date, DimJob.location)
            results = query.all()
            
            # Get unique locations and dates for frontend grid
            unique_locations = sorted(list(set([r.location for r in results if r.location])))
            unique_dates = sorted(list(set([str(r.date) for r in results])))
            
            data = []
            for r in results:
                data.append({
                    "date": str(r.date),
                    "location": r.location,
                    "shift_count": int(r.shift_count)
                })
            
            return jsonify({
                "view_type": "location_day",
                "locations": unique_locations,
                "dates": unique_dates,
                "data": data
            })
        
        else:
            # Calendar view (day-by-day)
            query = db.session.query(
                DimDate.date,
                DimDate.day,
                func.count(FactShift.shift_record_id).label('shift_count')
            ).select_from(FactShift)\
            .join(DimDate, FactShift.date_id == DimDate.date_id)\
            .filter(
                DimDate.date >= start,
                DimDate.date <= end
            )
            
            # Apply filters if location/site specified
            if locations or sites:
                query = query.join(DimJob, FactShift.job_id == DimJob.job_id)
                if locations:
                    query = query.filter(DimJob.location.in_(locations))
                if sites:
                    query = query.filter(DimJob.site.in_(sites))
            
            # RBAC: Filter by user's locations if not admin
            if current_user.role != 'admin':
                if not (locations or sites):
                    query = query.join(DimJob, FactShift.job_id == DimJob.job_id)
                
                try:
                    import json
                    user_locations = json.loads(current_user.location) if current_user.location else []
                    if user_locations:
                        query = query.filter(DimJob.location.in_(user_locations))
                except Exception as e:
                    current_app.logger.error(f"RBAC error in shifts-heatmap: {e}")
            
            query = query.group_by(DimDate.date, DimDate.day).order_by(DimDate.date)
            results = query.all()
            
            data = []
            for r in results:
                data.append({
                    "date": str(r.date),
                    "day_of_week": r.day,
                    "shift_count": int(r.shift_count)
                })
            
            return jsonify({
                "view_type": "calendar",
                "data": data
            })
    
    except Exception as e:
        current_app.logger.error(f"Error in shifts-heatmap API: {e}")
        return jsonify({"error": str(e)}), 500

@main_bp.route("/api/dashboard/hours-distribution")
@login_required
def api_hours_distribution():
    """
    Get distribution statistics (box plot data) for hours worked.
    Returns Min, Q1, Median, Q3, Max, and Outliers for selected dimension.
    """
    try:
        start = request.args.get("start")
        end = request.args.get("end")
        dimension = request.args.get("dimension", "client")  # client, site, staff
        limit = int(request.args.get("limit", 20))
        
        if not start or not end:
            return jsonify({"error": "Missing start/end date parameters"}), 400
            
        # Select appropriate grouping column and joins
        query = db.session.query(FactShift.paid_hours).join(DimDate, FactShift.date_id == DimDate.date_id)
        
        if dimension == 'client':
            group_col = DimClient.client_name
            name_label = 'Client'
            query = query.add_columns(group_col.label('entity'))
            query = query.join(DimClient, FactShift.client_id == DimClient.client_id)
            # Need DimJob for filters potentially
            query = query.join(DimJob, FactShift.job_id == DimJob.job_id)
            
        elif dimension == 'site':
            group_col = DimJob.site
            name_label = 'Site'
            query = query.add_columns(group_col.label('entity'))
            query = query.join(DimJob, FactShift.job_id == DimJob.job_id)
            
        elif dimension == 'staff':
            group_col = DimEmployee.full_name
            name_label = 'Staff'
            query = query.add_columns(group_col.label('entity'))
            query = query.join(DimEmployee, FactShift.employee_id == DimEmployee.employee_id)
            # Join DimJob for location filters
            query = query.join(DimJob, FactShift.job_id == DimJob.job_id)
        else:
            return jsonify({"error": "Invalid dimension"}), 400
            
        query = query.filter(
            DimDate.date >= start,
            DimDate.date <= end,
            FactShift.paid_hours > 0
        )
        
        # Apply filters
        query = apply_dashboard_filters(query)
        
        # Fetch all data
        results = query.all()
        
        # Process in Python (pandas is better for quartiles/outliers)
        import pandas as pd
        import numpy as np
        
        if not results:
            return jsonify([])
            
        # Results will be tuples like (hours, entity) due to add_columns
        # Note: query(FactShift.hours).add_columns(...) results in (hours, entity)
        # But let's be safe and check structure or use named tuples if possible, 
        # but standardized sqlalchemy result rows work fine.
        
        data_list = [{'hours': r[0], 'entity': r[1]} for r in results if r[1] is not None]
            
        if not data_list:
            return jsonify([])

        df = pd.DataFrame(data_list)
        df['hours'] = pd.to_numeric(df['hours'])
        
        # Group by entity
        grouped = df.groupby('entity')['hours'].apply(list).reset_index()
        
        # Calculate stats
        check_data = []
        
        for _, row in grouped.iterrows():
            entity = row['entity']
            values = sorted(row['hours'])
            
            if not values:
                continue
                
            q1 = np.percentile(values, 25)
            median = np.percentile(values, 50)
            q3 = np.percentile(values, 75)
            # Box plot min/max (usually 1.5*IQR)
            iqr = q3 - q1
            lower_bound = q1 - (1.5 * iqr)
            upper_bound = q3 + (1.5 * iqr)
            
            # Find actual min/max within bounds
            non_outliers = [v for v in values if v >= lower_bound and v <= upper_bound]
            min_val = min(non_outliers) if non_outliers else q1
            max_val = max(non_outliers) if non_outliers else q3
            
            # Identify outliers
            outliers = [v for v in values if v < lower_bound or v > upper_bound]
            
            # Calculate total hours for sorting
            total_hours = sum(values)
            
            check_data.append({
                "name": entity,
                "min": float(min_val),
                "q1": float(q1),
                "median": float(median),
                "q3": float(q3),
                "max": float(max_val),
                "outliers": [float(o) for o in outliers],
                "total_hours": float(total_hours),
                "count": len(values)
            })
            
        # Sort by median hours desc and take top N
        check_data.sort(key=lambda x: x['median'], reverse=True)
        return jsonify(check_data[:limit])
        
    except Exception as e:
        current_app.logger.error(f"Error in hours-distribution API: {e}")
        import traceback
        current_app.logger.error(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@main_bp.route("/api/dashboard/target-achievement-bullet")
@login_required
def api_target_achievement_bullet():
    try:
        start_date_str = request.args.get('start', type=str)
        end_date_str = request.args.get('end', type=str)
        dimension = request.args.get('dimension', 'site')
        limit = request.args.get('limit', 15, type=int)

        if not start_date_str or not end_date_str:
            # Default to current month
            now = datetime.now()
            start_date = datetime(now.year, now.month, 1)
            end_date = now
            start_date_str = start_date.strftime('%Y-%m-%d')
            end_date_str = end_date.strftime('%Y-%m-%d')
        else:
            try:
                datetime.strptime(start_date_str, '%Y-%m-%d')
                datetime.strptime(end_date_str, '%Y-%m-%d')
            except ValueError:
                return jsonify({"error": "Invalid date format. Use YYYY-MM-DD"}), 400

        # Determine Grouping
        if dimension == 'client':
            group_col = DimClient.client_name
            join_table = DimClient
            join_cond = FactShift.client_id == DimClient.client_id
        elif dimension == 'site':
            group_col = DimJob.site
            join_table = DimJob
            join_cond = FactShift.job_id == DimJob.job_id
        else:
             return jsonify({"error": "Invalid dimension"}), 400

        # 1. Get Actual Shifts
        actuals_query = db.session.query(
            group_col.label('name'),
            func.count(FactShift.shift_record_id).label('actual_count')
        ).join(join_table, join_cond)\
         .join(DimDate, FactShift.date_id == DimDate.date_id)\
         .filter(DimDate.date >= start_date_str, DimDate.date <= end_date_str)
        
        # RBAC and Join for Client dimension if not Admin
        if current_user.role != 'admin':
            # Ensure DimJob is joined for location/site filtering
            if dimension == 'client':
                actuals_query = actuals_query.join(DimJob, FactShift.job_id == DimJob.job_id)
            
            if current_user.location:
                actuals_query = actuals_query.filter(DimJob.location == current_user.location)
            if current_user.site:
                actuals_query = actuals_query.filter(DimJob.site == current_user.site)

        actuals = actuals_query.group_by(group_col).all()
        actual_map = {r.name: r.actual_count for r in actuals if r.name}

        # 2. Get Targets (Only for Site)
        target_map = {}
        if dimension == 'site':
            dates = db.session.query(DimDate.year, DimDate.month)\
                .filter(DimDate.date >= start_date_str, DimDate.date <= end_date_str)\
                .distinct().all()
            
            if dates:
                for year, month in dates:
                    targets_query = ShiftTarget.query.filter_by(year=year, month=month)
                    if current_user.role != 'admin':
                        if current_user.location:
                             targets_query = targets_query.filter_by(location=current_user.location)
                        if current_user.site:
                             targets_query = targets_query.filter_by(site=current_user.site)
                    
                    for t in targets_query.all():
                        site_name = t.site
                        if not site_name: continue
                        target_map[site_name] = target_map.get(site_name, 0) + t.target_count

        # 3. Merge and Format
        results = []
        all_keys = set(actual_map.keys()) | set(target_map.keys())
        
        for key in all_keys:
            if not key: continue

            actual = actual_map.get(key, 0)
            target = target_map.get(key, 0)
            
            if actual == 0 and target == 0:
                continue
            
            if target == 0:
                achievement = 100 if actual > 0 else 0
                calc_reference = actual if actual > 0 else 100
            else:
                achievement = (actual / target) * 100
                calc_reference = target

            range_poor = calc_reference * 0.8
            range_good = calc_reference * 0.15 
            range_excellent = calc_reference * 0.30

            results.append({
                "name": key,
                "actual": actual,
                "target": target,
                "achievement": round(achievement, 1),
                "range_poor": range_poor,
                "range_good": range_good,
                "range_excellent": range_excellent, 
                "marker": target 
            })
            
        # Sort by Actual desc
        results.sort(key=lambda x: x['actual'], reverse=True)
        
        return jsonify(results[:limit])

    except Exception as e:
        current_app.logger.error(f"Error in target achievement: {e}")
        return jsonify({"error": str(e)}), 500


@main_bp.route("/api/dashboard/revenue-waterfall")
@login_required
def api_revenue_waterfall():
    try:
        start_date_str = request.args.get('start', type=str)
        end_date_str = request.args.get('end', type=str)
        
        if not start_date_str or not end_date_str:
             return jsonify({"error": "Start and End dates required"}), 400

        try:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
        except ValueError:
            return jsonify({"error": "Invalid date format"}), 400

        # Calculate Previous Period
        duration = end_date - start_date
        # Simple Logic: Previous period is same duration ending on start_date - 1 day
        prev_end = start_date - timedelta(days=1)
        prev_start = prev_end - duration
        
        prev_start_str = prev_start.strftime('%Y-%m-%d')
        prev_end_str = prev_end.strftime('%Y-%m-%d')

        def get_client_revenue(s_date, e_date):
            query = db.session.query(
                FactShift.client_id,
                func.sum(FactShift.client_net).label('revenue')
            ).join(DimDate, FactShift.date_id == DimDate.date_id)\
             .filter(DimDate.date >= s_date, DimDate.date <= e_date)
            
            # RBAC
            if current_user.role != 'admin':
                query = query.join(DimJob, FactShift.job_id == DimJob.job_id)
                if current_user.location:
                    query = query.filter(DimJob.location == current_user.location)
                if current_user.site:
                    query = query.filter(DimJob.site == current_user.site)
            
            # Apply Request Filters
            req_locations = request.args.getlist('locations')
            req_sites = request.args.getlist('sites')
            
            if req_locations or req_sites:
                # If not already joined DimJob (e.g. admin)
                is_admin = current_user.role == 'admin'
                has_joined_job = not is_admin 
                
                if not has_joined_job: 
                     query = query.join(DimJob, FactShift.job_id == DimJob.job_id)
                
                if req_locations:
                    query = query.filter(DimJob.location.in_(req_locations))
                if req_sites:
                    query = query.filter(DimJob.site.in_(req_sites))

            return {row.client_id: float(row.revenue or 0) for row in query.group_by(FactShift.client_id).all()}

        prev_rev_map = get_client_revenue(prev_start_str, prev_end_str)
        curr_rev_map = get_client_revenue(start_date_str, end_date_str)

        starting_rev = sum(prev_rev_map.values())
        ending_rev = sum(curr_rev_map.values())

        new_clients_rev = 0
        lost_clients_rev = 0
        net_movement_rev = 0

        all_clients = set(prev_rev_map.keys()) | set(curr_rev_map.keys())

        for client_id in all_clients:
            prev = prev_rev_map.get(client_id, 0)
            curr = curr_rev_map.get(client_id, 0)

            if prev == 0 and curr > 0:
                new_clients_rev += curr
            elif prev > 0 and curr == 0:
                lost_clients_rev += prev # Magnitude of loss
            elif prev > 0 and curr > 0:
                net_movement_rev += (curr - prev)
        
        # Format for Waterfall
        # Order: Starting, New (+), Lost (-), Net (+/-), Ending
        
        data = [
            {"name": "Starting Revenue", "value": starting_rev, "type": "start"},
            {"name": "New Clients", "value": new_clients_rev, "type": "plus"},
            {"name": "Lost Clients", "value": -lost_clients_rev, "type": "minus"},
            {"name": "Net Movement", "value": net_movement_rev, "type": "plus" if net_movement_rev >= 0 else "minus"},
            {"name": "Ending Revenue", "value": ending_rev, "type": "total"}
        ]

        return jsonify(data)
    except Exception as e:
        import traceback
        traceback.print_exc()
        print(f"DEBUG ERROR in revenue-waterfall: {e}", flush=True)
        current_app.logger.error(f"Error in revenue-waterfall: {e}")
        return jsonify({"error": str(e)}), 500


@main_bp.route("/api/dashboard/client-margin-treemap")
@login_required
def api_client_margin_treemap():
    try:
        start_date_str = request.args.get('start', type=str)
        end_date_str = request.args.get('end', type=str)
        
        if not start_date_str or not end_date_str:
             return jsonify({"error": "Start and End dates required"}), 400

        # Query
        query = db.session.query(
            DimClient.client_name,
            func.sum(FactShift.client_net).label('revenue'),
            func.sum(FactShift.total_pay).label('cost')
        ).join(DimClient, FactShift.client_id == DimClient.client_id)\
         .join(DimDate, FactShift.date_id == DimDate.date_id)\
         .filter(DimDate.date >= start_date_str, DimDate.date <= end_date_str)

        # RBAC
        if current_user.role != 'admin':
            query = query.join(DimJob, FactShift.job_id == DimJob.job_id)
            if current_user.location:
                query = query.filter(DimJob.location == current_user.location)
            if current_user.site:
                query = query.filter(DimJob.site == current_user.site)
        
        # Apply Request Filters
        req_locations = request.args.getlist('locations')
        req_sites = request.args.getlist('sites')
        
        target_metric = request.args.get('metric', 'revenue') # 'revenue' or 'cost'
        
        if req_locations or req_sites:
            # If not already joined DimJob
            if current_user.role == 'admin': 
                    query = query.join(DimJob, FactShift.job_id == DimJob.job_id)
            
            if req_locations:
                query = query.filter(DimJob.location.in_(req_locations))
            if req_sites:
                query = query.filter(DimJob.site.in_(req_sites))

        results = query.group_by(DimClient.client_name).all()

        data = []
        for r in results:
            revenue = float(r.revenue or 0)
            cost = float(r.cost or 0)
            profit = revenue - cost
            margin = (profit / revenue * 100) if revenue > 0 else 0
            
            # Determine size based on selected metric
            size_val = cost if target_metric == 'cost' else revenue

            if size_val > 0: # Only show clients with value > 0
                data.append({
                    "name": r.client_name,
                    "size": size_val, 
                    "revenue": revenue,
                    "cost": cost,
                    "profit": profit,
                    "margin": round(margin, 1)
                })
        
        # Sort by size desc
        data.sort(key=lambda x: x['size'], reverse=True)

        return jsonify(data)

    except Exception as e:
        current_app.logger.error(f"Error in client-margin-treemap: {e}")
        return jsonify({"error": str(e)}), 500

@main_bp.route("/api/dashboard/profit-variance", methods=["GET"])
@login_required
def api_profit_variance():
    """
    Returns Monthly Actual Profit vs Target Profit.
    Actual Profit = Sum(Client Net) - Sum(Total Pay) from FactShift.
    Target Profit = Value from FinancialMetric where name is 'Profit Target' (or 'Net Profit Budget').
    """
    try:
        start_date_str = request.args.get('start')
        end_date_str = request.args.get('end')
        
        if not start_date_str or not end_date_str:
            return jsonify({"error": "Start and End dates required"}), 400

        # 1. Calculate Actual Profit by Month
        query = db.session.query(
            DimDate.year,
            DimDate.month,
            func.sum(FactShift.client_net).label('revenue'),
            func.sum(FactShift.total_pay).label('cost')
        ).join(
            DimDate, FactShift.date_id == DimDate.date_id
        ).filter(
            DimDate.date >= start_date_str,
            DimDate.date <= end_date_str
        )
        
        # RBAC and Filters
        req_locations = request.args.getlist('locations')
        req_sites = request.args.getlist('sites')
        
        if req_locations or req_sites or (current_user.role != 'admin' and current_user.location):
             query = query.join(DimJob, FactShift.job_id == DimJob.job_id)
             
             if current_user.role != 'admin' and current_user.location:
                # Basic RBAC for now
                query = query.filter(DimJob.location == current_user.location)
             
             if req_locations:
                 query = query.filter(DimJob.location.in_(req_locations))
             if req_sites:
                 query = query.filter(DimJob.site.in_(req_sites))
                 
        actuals = query.group_by(DimDate.year, DimDate.month).all()
        
        actual_map = {} # Key: "YYYY-MonthName"
        for r in actuals:
            rev = r.revenue or 0
            cost = r.cost or 0
            profit = rev - cost
            key = f"{r.year}-{r.month}"
            actual_map[key] = profit

        # 2. Fetch Targets from FinancialMetric
        # Assuming we look for a metric named "Profit Target" or similar.
        # We need to filter by year range.
        try:
            start_year = int(start_date_str[:4])
        except:
            start_year = datetime.now().year

        target_query = FinancialMetric.query.filter(
            FinancialMetric.year >= start_year,
            FinancialMetric.name.ilike('%Profit Target%') # Flexible matching
        )
        
        # Apply filters to targets too?
        # FinancialMetric has location/site.
        if req_locations:
            target_query = target_query.filter(FinancialMetric.location.in_(req_locations))
        elif current_user.role != 'admin' and current_user.location:
             target_query = target_query.filter(FinancialMetric.location == current_user.location)
             
        targets = target_query.all()
        
        target_map = {}
        for t in targets:
            key = f"{t.year}-{t.month}"
            # Sum targets if multiple entries (e.g. multiple sites)
            target_map[key] = target_map.get(key, 0) + (t.value or 0)
            
        # 3. Combine Data
        # We need a list of all relevant months.
        # Let's iterate through actuals and targets to build a complete list.
        all_keys = set(actual_map.keys()) | set(target_map.keys())
        
        data = []
        months_order = ["January", "February", "March", "April", "May", "June", 
                        "July", "August", "September", "October", "November", "December"]

        for key in all_keys:
            parts = key.split('-')
            year = int(parts[0])
            month = parts[1]
            
            actual = actual_map.get(key, 0)
            target = target_map.get(key, 0) # Default to 0 if not set
            variance = actual - target
            
            data.append({
                "year": year,
                "month": month,
                "display": f"{month[:3]} {year}",
                "actual": actual,
                "target": target,
                "variance": variance,
                "variancePercent": (variance / target * 100) if target > 0 else 0
            })
            
        # Sort
        data.sort(key=lambda x: (x['year'], months_order.index(x['month']) if x['month'] in months_order else 0))
        
        return jsonify(data)

    except Exception as e:
        current_app.logger.error(f"Error in profit-variance: {e}")
        return jsonify({"error": str(e)}), 500
        return jsonify({"error": str(e)}), 500

@main_bp.route("/api/financial-summary/metrics", methods=["GET"])
@login_required
def api_financial_summary_metrics():
    """
    Returns aggregated financial metrics for the summary table.
    - Total Sales (SUM(client_net))
    - Cost of Sales PAYE Gross (SUM(total_pay))
    Grouped by Month.
    """
    try:
        start_date_str = request.args.get('start')
        end_date_str = request.args.get('end')

        if not start_date_str or not end_date_str:
             return jsonify({"error": "Start and End dates required"}), 400

        # Query FactShifts
        query = db.session.query(
            DimDate.year,
            DimDate.month,
            func.sum(FactShift.client_net).label('revenue'),
            func.sum(case((FactShift.self_employed == True, FactShift.total_pay), else_=0)).label('cost_se'),
            func.sum(case((FactShift.self_employed == False, FactShift.total_pay), else_=0)).label('cost_paye'),
            func.sum(FactShift.paid_hours).label('total_hours')
        ).join(
            DimDate, FactShift.date_id == DimDate.date_id
        ).filter(
            DimDate.date >= start_date_str,
            DimDate.date <= end_date_str
        )
        
        # RBAC
        if current_user.role != 'admin' and current_user.location:
             query = query.join(DimJob, FactShift.job_id == DimJob.job_id)
             query = query.filter(DimJob.location == current_user.location)
             

        
        # Apply Request Filters if any (though usually this page is high-level)
        # keeping it consistent with other dashboards just in case
        req_locations = request.args.getlist('locations')
        req_sites = request.args.getlist('sites')

        if req_locations or req_sites:
             # Check if we need to join DimJob (if admin didn't already)
             if current_user.role == 'admin':
                 query = query.join(DimJob, FactShift.job_id == DimJob.job_id)
             
             if req_locations:
                 query = query.filter(DimJob.location.in_(req_locations))
             if req_sites:
                 query = query.filter(DimJob.site.in_(req_sites))

        results = query.group_by(DimDate.year, DimDate.month).all()
        
        data = {}
        for r in results:
            # key: "2025-January"
            # We need month name. DimDate.month is integer.
            # We need month name. DimDate.month might be 'January' or '1'
            m_val = r.month
            try:
                # Try to convert to int if it looks like a number
                m_int = int(m_val)
                month_name = datetime(2000, m_int, 1).strftime('%B')
            except (ValueError, TypeError):
                # It's likely already a string name like "January"
                month_name = str(m_val) if m_val else "Unknown"

            key = f"{r.year}-{month_name}"
            
            data[key] = {
                "total_sales": float(r.revenue or 0),
                "cost_sales_se": float(r.cost_se or 0),
                "cost_sales_paye": float(r.cost_paye or 0),
                "total_hours": float(r.total_hours or 0)
            }
        
        return jsonify(data)

    except Exception as e:
        current_app.logger.error(f"Error in financial-summary/metrics: {e}")
        return jsonify({"error": str(e)}), 500


@main_bp.route("/api/financial-summary/save", methods=["POST"])
@login_required
def api_save_financial_summary():
    """
    Saves financial summary data.
    Expected JSON:
    {
        "year": 2025,
        "data": {
            "row_id": { "col_id": value, ... },
            ...
        },
        "custom_rows": [ { "id": "...", "label": "..." }, ... ]
    }
    """
    try:
        payload = request.get_json()
        year = payload.get('year', datetime.now().year)
        data = payload.get('data', {})
        custom_rows = payload.get('custom_rows', [])
        
        # 1. Update FinancialMetric values
        # We process 'data' to find values for each row/month
        # Assuming row_id maps to metric name, col_id maps to month (jan, feb, ...)
        
        month_map = {
            "jan": "January", "feb": "February", "mar": "March", "apr": "April",
            "may": "May", "jun": "June", "jul": "July", "aug": "August",
            "sep": "September", "oct": "October", "nov": "November", "dec": "December",
            # Year end might be calculated, usually not saved as a separate metric entry unless specified
        }

        # Clear existing metrics for this year? Or just functionality to update/upsert?
        # For now, let's upsert based on (name, year, month, location)
        # Assuming global location for simplicity unless passed in request
        
        # We need a mapping from row_id to metric name. 
        # If it's a default row, we use a known map? Or just use the label?
        # Let's use the label passed from frontend (we might need to send row definitions)
        
        # Ideally, frontend sends: [ { name: "Total Sales", month: "January", value: 123 }, ... ]
        # But here we receive the matrix.
        
        # Let's trust the frontend to send a structured list of metrics to save?
        # Or parse the matrix.
        
        # Simplified approach: Frontend sends list of metrics to upsert.
        # But the payload structure in docstring says 'data' matrix.
        
        # Let's act on the payload.
        # We need row labels. 
        # The 'custom_rows' tells us about new rows, but what about default rows?
        # The frontend should probably send `rows` config in payload.
        
        rows_config = payload.get('rows', [])
        row_label_map = { r['id']: r['label'] for r in rows_config }
        
        changes_count = 0
        
        for row_id, cols in data.items():
            metric_name = row_label_map.get(row_id)
            if not metric_name: continue
            
            # recursive dictionary in payload? data[row_id][col_id] = { value: ... }
            for col_id, cell_data in cols.items():
                if col_id not in month_map: continue
                
                try:
                    val = cell_data.get('value')
                    if val is None: continue
                    val = float(val)
                except:
                    continue
                    
                month_name = month_map[col_id]
                
                # Find or create metric
                metric = FinancialMetric.query.filter_by(
                    name=metric_name,
                    year=year,
                    month=month_name,
                    # Location/Site? defaulting to None (Global) or current user?
                    # Let's assume Global for "Financial Summary" unless specified
                    location=None, 
                    site=None
                ).first()
                
                if metric:
                    metric.value = val
                else:
                    metric = FinancialMetric(
                        name=metric_name,
                        value=val,
                        year=year,
                        month=month_name,
                        metric_type='financial', # generic type
                        location=None,
                        site=None
                    )
                    db.session.add(metric)
                changes_count += 1
                
        db.session.commit()
        
        return jsonify({"message": "Saved successfully", "changes": changes_count})

    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Error saving financial summary: {e}")
        return jsonify({"error": str(e)}), 500


# ─── Financial Summary Cell Overrides ────────────────────────────────────────

from .models import FinancialSummaryOverride

@main_bp.route("/api/financial-summary/overrides", methods=["GET"])
@login_required
def get_financial_summary_overrides():
    """Return all saved manual cell overrides."""
    overrides = FinancialSummaryOverride.query.all()
    return jsonify([
        {"row_id": o.row_id, "col_id": o.col_id, "value": o.value}
        for o in overrides
    ])


@main_bp.route("/api/financial-summary/overrides", methods=["POST"])
@login_required
def save_financial_summary_override():
    """Upsert a single cell override (row_id + col_id → value)."""
    data = request.get_json()
    row_id = data.get("row_id")
    col_id = data.get("col_id")
    value  = data.get("value")   # None means clear the override

    if not row_id or not col_id:
        return jsonify({"error": "row_id and col_id are required"}), 400

    existing = FinancialSummaryOverride.query.filter_by(row_id=row_id, col_id=col_id).first()

    try:
        if value is None:
            # Clear the override
            if existing:
                db.session.delete(existing)
        else:
            if existing:
                existing.value = float(value)
            else:
                db.session.add(FinancialSummaryOverride(
                    row_id=row_id,
                    col_id=col_id,
                    value=float(value)
                ))
        db.session.commit()
        return jsonify({"ok": True})
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Error saving override: {e}")
        return jsonify({"error": str(e)}), 500
