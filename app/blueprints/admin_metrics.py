from flask import Blueprint, request, jsonify, current_app
from flask_login import login_required, current_user
from app.models import db, DimDate, DimJob, FactShift, FinancialMetric, ShiftTarget
from app.auth import admin_required
from sqlalchemy import func

admin_metrics_bp = Blueprint("admin_metrics", __name__)

@admin_metrics_bp.route("/api/admin/targets", methods=["GET"])
@login_required
@admin_required
def get_targets():
    try:
        year = request.args.get('year', type=int)
        
        query = ShiftTarget.query
        if year:
            query = query.filter_by(year=year)
            
        targets = query.all()
        return jsonify([{
            "id": t.id,
            "year": t.year,
            "month": t.month,
            "location": t.location,
            "site": t.site,
            "target": t.target_count
        } for t in targets])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@admin_metrics_bp.route("/api/admin/targets", methods=["POST"])
@login_required
@admin_required
def set_target():
    try:
        data = request.json
        # Expect: year, month, location, site (optional), target
        
        # Normalize site: empty string -> None
        site_val = data.get('site')
        if site_val == "":
            site_val = None
        
        # Check if exists
        target = ShiftTarget.query.filter_by(
            year=data['year'],
            month=data['month'],
            location=data['location'],
            site=site_val
        ).first()
        
        if target:
            target.target_count = data['target']
        else:
            target = ShiftTarget(
                year=data['year'],
                month=data['month'],
                location=data['location'],
                site=site_val,
                target_count=data['target']
            )
            db.session.add(target)
            
        db.session.commit()
        return jsonify({"message": "Target saved successfully"}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500

@admin_metrics_bp.route("/api/admin/financial-metrics", methods=["GET"])
@login_required
@admin_required
def get_financial_metrics():
    try:
        year = request.args.get('year', type=int)
        
        query = FinancialMetric.query
        if year:
            query = query.filter_by(year=year)
            
        metrics = query.all()
        return jsonify([{
            "id": m.id,
            "year": m.year,
            "month": m.month,
            "name": m.name,
            "value": m.value,
            "location": m.location,
            "site": m.site
        } for m in metrics])
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@admin_metrics_bp.route("/api/admin/financial-metrics", methods=["POST"])
@login_required
@admin_required
def set_financial_metric():
    try:
        data = request.json
        
        # Normalize empty strings to None
        location_val = data.get('location') if data.get('location') else None
        site_val = data.get('site') if data.get('site') else None
        
        # Check if exists
        metric = FinancialMetric.query.filter_by(
            year=data['year'],
            month=data['month'],
            name=data['name'],
            location=location_val,
            site=site_val
        ).first()
        
        if metric:
            metric.value = data['value']
        else:
            metric = FinancialMetric(
                year=data['year'],
                month=data['month'],
                name=data['name'],
                value=data['value'],
                location=location_val,
                site=site_val
            )
            db.session.add(metric)
            
        db.session.commit()
        return jsonify({"message": "Financial metric saved successfully"}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500

@admin_metrics_bp.route("/api/admin/financial-metrics/<int:id>", methods=["DELETE"])
@login_required
@admin_required
def delete_financial_metric(id):
    try:
        metric = FinancialMetric.query.get(id)
        if not metric:
            return jsonify({"error": "Metric not found"}), 404
            
        db.session.delete(metric)
        db.session.commit()
        return jsonify({"message": "Metric deleted successfully"}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500

@admin_metrics_bp.route("/api/dashboard/targets-performance", methods=["GET"])
@login_required
def get_target_performance():
    """
    Returns actual vs target for a given date range.
    Note: Simplified to Monthly view for now.
    """
    try:
        start_date = request.args.get('start') # YYYY-MM-DD
        end_date = request.args.get('end')
        
        # 0. Get Site Counts per Location (for target division)
        site_counts_query = db.session.query(
            DimJob.location,
            func.count(func.distinct(DimJob.site))
        ).group_by(DimJob.location).all()
        
        # Map location -> site_count
        loc_site_counts = {loc: count for loc, count in site_counts_query}

        # 1. Get Actual counts per Location/Site/Month
        # Group by Year, Month, Location, Site
        
        actual_query = db.session.query(
            DimDate.year,
            DimDate.month,
            DimJob.location,
            DimJob.site,
            func.count(FactShift.shift_record_id).label('actual_count')
        ).join(
            DimDate, FactShift.date_id == DimDate.date_id
        ).join(
            DimJob, FactShift.job_id == DimJob.job_id
        ).filter(
            DimDate.date >= start_date,
            DimDate.date <= end_date
        )

        # RBAC Filter for non-admins
        if current_user.role not in ['admin', 'superadmin']:
            if current_user.location:
                # Parse location JSON if needed
                import json
                try:
                    user_locs = json.loads(current_user.location)
                    if isinstance(user_locs, list):
                        actual_query = actual_query.filter(DimJob.location.in_(user_locs))
                    else:
                        # Fallback if it's a single string (legacy or simple case)
                        actual_query = actual_query.filter(DimJob.location == str(user_locs))
                except (json.JSONDecodeError, TypeError):
                     # Not JSON, treat as exact string match
                     actual_query = actual_query.filter(DimJob.location == current_user.location)
        
        actual_query = actual_query.group_by(
            DimDate.year, DimDate.month, DimJob.location, DimJob.site
        ).all()
        
        # 2. Get Targets for the relevant months/years
        # This is strictly not filtering by exact range yet, optimization possible
        # For MVP, fetch all targets and map in python? Or filter by year range.
        
        start_dt = start_date.split('-')
        year_start = int(start_dt[0])
        
        targets = ShiftTarget.query.filter(ShiftTarget.year >= year_start).all()
        
        # 3. Map and Compare
        performance = []
        
        # Create a lookup for targets: key = (year, month, location, site)
        target_map = {}
        for t in targets:
            # IMPORTANT: Treat None site as None key, do not coerce to 'ALL'
            key = (t.year, t.month, t.location, t.site) 
            target_map[key] = t.target_count

        # Logic: We need to see if a site met its target.
        # If site-specific target exists, use it.
        # If only location-target exists, we need to handle "distribute evenly" or "aggregate".
        # Requirement: "if not specified for each site, divide the total amout to each sites"
        
        # Let's process Actuals
        processed_sites = set()
        
        for row in actual_query:
            year, month, loc, site, actual = row
            # Site might be None from DB if not set
            key = (year, month, loc, site)
            processed_sites.add(key)
            
            # Check Site Target
            target_site = target_map.get((year, month, loc, site))
            
            final_target = target_site
            
            if final_target is None:
                # Default to 1000 if no target configured
                final_target = 1000
            
            performance.append({
                "year": year,
                "month": month,
                "location": loc,
                "site": site,
                "actual": actual,
                "target": final_target
            })

        return jsonify(performance)

    except Exception as e:
        return jsonify({"error": str(e)}), 500


@admin_metrics_bp.route("/api/dashboard/target-achievement-trend", methods=["GET"])
@login_required
def get_target_achievement_trend():
    """
    Returns achievement percentage trend over time.
    Aggregates all sites per period to show overall achievement %.
    """
    try:
        start_date = request.args.get('start')  # YYYY-MM-DD
        end_date = request.args.get('end')
        
        if not start_date or not end_date:
            return jsonify({"error": "start and end dates required"}), 400
        
        # Get actual performance data
        actual_query = db.session.query(
            DimDate.year,
            DimDate.month,
            DimJob.location,
            DimJob.site,
            func.count(FactShift.shift_record_id).label('actual_count')
        ).join(
            DimDate, FactShift.date_id == DimDate.date_id
        ).join(
            DimJob, FactShift.job_id == DimJob.job_id
        ).filter(
            DimDate.date >= start_date,
            DimDate.date <= end_date
        )

        # RBAC Filter
        if current_user.role not in ['admin', 'superadmin']:
            if current_user.location:
                import json
                try:
                    user_locs = json.loads(current_user.location)
                    if isinstance(user_locs, list):
                        actual_query = actual_query.filter(DimJob.location.in_(user_locs))
                    else:
                        actual_query = actual_query.filter(DimJob.location == str(user_locs))
                except (json.JSONDecodeError, TypeError):
                     actual_query = actual_query.filter(DimJob.location == current_user.location)
        
        actual_query = actual_query.group_by(
            DimDate.year, DimDate.month, DimJob.location, DimJob.site
        ).all()
        
        # Get targets - filter by year range
        start_year = int(start_date[:4])
        targets = ShiftTarget.query.filter(ShiftTarget.year >= start_year).all()
        
        # Build target map
        target_map = {}
        for t in targets:
            key = (t.year, t.month, t.location, t.site)
            target_map[key] = t.target_count
        
        # Aggregate by period (year-month)
        period_data = {}
        
        for row in actual_query:
            year, month, loc, site, actual = row
            period_key = f"{month} {year}"
            
            # Get target (default to 1000 if not manually configured)
            # This implements the business rule: 1000 shifts per site (or per location if no site)
            target = target_map.get((year, month, loc, site), 1000)
            
            if period_key not in period_data:
                period_data[period_key] = {
                    "year": year,
                    "month": month,
                    "totalActual": 0,
                    "totalTarget": 0,
                    "sitesMetTarget": 0,
                    "totalSites": 0
                }
            
            period_data[period_key]["totalActual"] += actual
            period_data[period_key]["totalTarget"] += target
            period_data[period_key]["totalSites"] += 1
            
            if actual >= target:
                period_data[period_key]["sitesMetTarget"] += 1
        
        # If no data at all, return empty array rather than error
        if not period_data:
            return jsonify({"data": []})
        
        # Sort by date and format response
        month_order = ["January", "February", "March", "April", "May", "June",
                       "July", "August", "September", "October", "November", "December"]
        
        sorted_data = sorted(
            period_data.values(),
            key=lambda x: (x["year"], month_order.index(x["month"]))
        )
        
        # Calculate achievement percentage
        result = []
        for item in sorted_data:
            achievement_pct = (item["sitesMetTarget"] / item["totalSites"] * 100) if item["totalSites"] > 0 else 0
            
            result.append({
                "period": f"{item['month'][:3]} {item['year']}",
                "display": f"{item['month'][:3]} {item['year']}",
                "achievementPercentage": round(achievement_pct, 1),
                "sitesMetTarget": item["sitesMetTarget"],
                "totalSites": item["totalSites"],
                "totalActual": item["totalActual"],
                "totalTarget": item["totalTarget"]
            })
        
        return jsonify({"data": result})

    except Exception as e:
        return jsonify({"error": str(e)}), 500
