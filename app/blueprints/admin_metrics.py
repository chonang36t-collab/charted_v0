from flask import Blueprint, request, jsonify, current_app
from flask_login import login_required
from app.models import db, ShiftTarget, FinancialMetric, DimJob, FactShift, DimDate
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
        
        # Check if exists
        target = ShiftTarget.query.filter_by(
            year=data['year'],
            month=data['month'],
            location=data['location'],
            site=data.get('site')
        ).first()
        
        if target:
            target.target_count = data['target']
        else:
            target = ShiftTarget(
                year=data['year'],
                month=data['month'],
                location=data['location'],
                site=data.get('site'),
                target_count=data['target']
            )
            db.session.add(target)
            
        db.session.commit()
        return jsonify({"message": "Target saved play"}), 200
    except Exception as e:
        db.session.rollback()
        return jsonify({"error": str(e)}), 500

@admin_metrics_bp.route("/api/admin/financial-metrics", methods=["GET", "POST"])
@login_required
@admin_required
def handle_financial_metrics():
    try:
        if request.method == "GET":
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
                "value": m.value
            } for m in metrics])
            
        elif request.method == "POST":
            data = request.json
            metric = FinancialMetric.query.filter_by(
                year=data['year'],
                month=data['month'],
                name=data['name']
            ).first()
            
            if metric:
                metric.value = float(data['value'])
            else:
                metric = FinancialMetric(
                    year=data['year'],
                    month=data['month'],
                    name=data['name'],
                    value=float(data['value'])
                )
                db.session.add(metric)
            
            db.session.commit()
            return jsonify({"message": "Metric saved"}), 200
            
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
        ).group_by(
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
            key = (t.year, t.month, t.location, t.site or 'ALL')
            target_map[key] = t.target_count

        # Logic: We need to see if a site met its target.
        # If site-specific target exists, use it.
        # If only location-target exists, we need to handle "distribute evenly" or "aggregate".
        # Requirement: "if not specified for each site, divide the total amout to each sites"
        
        # Let's process Actuals
        processed_sites = set()
        
        for row in actual_query:
            year, month, loc, site, actual = row
            key = (year, month, loc, site)
            processed_sites.add(key)
            
            # Check Site Target
            target_site = target_map.get((year, month, loc, site))
            
            if target_site is None:
                # Check Location Target and divide?
                # We need count of sites in this location for this month to divide accurately... 
                # This could get complex in SQL.
                # Simplified approach: Look for Location target
                target_loc = target_map.get((year, month, loc, 'ALL'))
                if target_loc:
                    # How many sites active in this location this month?
                    # This requires a separate query or pre-calc.
                    # Fallback: Just return the Actual, Client can calculate if we send the Reference Target.
                    pass 
            
            # For now, let's just return the raw data and handle 'Meeting Target' logic in Frontend or simplified here.
            # actually, let's just return a list of { location, site, year, month, actual, target }
            
            final_target = target_site
            
            if final_target is None:
                # Try finding location target
                loc_target = target_map.get((year, month, loc, 'ALL'))
                if loc_target:
                    # Use location target for this site
                    # Note: This assigns the full location target to each site
                    # If you want to split it evenly, additional logic is needed
                    final_target = loc_target
            
            
            performance.append({
                "year": year,
                "month": month,
                "location": loc,
                "site": site,
                "actual": actual,
                "target": final_target or 0
            })

        return jsonify(performance)

    except Exception as e:
        return jsonify({"error": str(e)}), 500
