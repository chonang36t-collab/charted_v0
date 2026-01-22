from flask import Blueprint, request, jsonify, current_app
from flask_login import login_required, current_user
from sqlalchemy import or_, desc, asc, text
from app.models import db, FactShift, DimEmployee, DimClient, DimJob, DimDate, DimShift
from app.utils.filters import apply_dashboard_filters
from datetime import datetime

records_bp = Blueprint("records", __name__)

@records_bp.route("/api/records")
@login_required
def api_records():
    """
    Get paginated, sorted, and filtered shift records.
    Params:
    - page: int (default 1)
    - limit: int (default 50)
    - sort_by: str (default 'date')
    - sort_order: 'asc' | 'desc' (default 'desc')
    - search: str (global search)
    - start, end: Date range
    - locations, sites, clients: Filters
    """
    try:
        # 1. Parse Parameters
        page = request.args.get('page', 1, type=int)
        limit = request.args.get('limit', 50, type=int)
        sort_by = request.args.get('sort_by', 'date')
        sort_order = request.args.get('sort_order', 'desc')
        search = request.args.get('search', '').lower()
        start = request.args.get('start')
        end = request.args.get('end')

        # 2. Build Base Query
        query = db.session.query(
            FactShift,
            DimEmployee.full_name,
            DimClient.client_name,
            DimJob.location,
            DimJob.site,
            DimDate.date,
            DimShift.shift_name,
            DimShift.shift_start,
            DimShift.shift_end
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

        # 3. Apply Filters
        
        # Date Range
        if start:
            query = query.filter(DimDate.date >= start)
        if end:
            query = query.filter(DimDate.date <= end)
            
        # Standard Dashboard Filters (Locations, Sites, Clients from query params)
        # Note: apply_dashboard_filters relies on request.args directly
        query = apply_dashboard_filters(query)
        
        # Global Search
        if search:
            search_term = f"%{search}%"
            query = query.filter(or_(
                DimEmployee.full_name.ilike(search_term),
                DimClient.client_name.ilike(search_term),
                DimJob.location.ilike(search_term),
                DimJob.site.ilike(search_term),
                DimShift.shift_name.ilike(search_term)
            ))

        # 4. Sorting
        # Map sort_by to actual columns
        sort_map = {
            'date': DimDate.date,
            'full_name': DimEmployee.full_name,
            'client_name': DimClient.client_name,
            'location': DimJob.location,
            'site': DimJob.site,
            'shift_name': DimShift.shift_name,
            'paid_hours': FactShift.paid_hours,
            'total_pay': FactShift.total_pay,
            'client_net': FactShift.client_net,
            # Add more as needed
        }
        
        col = sort_map.get(sort_by, DimDate.date)
        if sort_order == 'asc':
            query = query.order_by(asc(col))
        else:
            query = query.order_by(desc(col))
            
        # 5. Pagination
        pagination = query.paginate(page=page, per_page=limit, error_out=False)
        
        # 6. Serialize
        data = []
        for row in pagination.items:
            fact = row[0]
            # row: [FactShift, full_name, client_name, location, site, date, shift_name, start, end]
            
            # Role-based masking for managers (hide margin/costs? or just financial summary?)
            # Usually record view needs detailed costs for auditing, but let's follow dashboard rule:
            # If viewer/manager, maybe hide 'total_pay' (cost) if sensitive? 
            # For now, we return everything, assuming "Records" view users have appropriate clearance or we rely on location RBAC.
            # If strict financial hiding is needed, we mask it here.
            
            item = {
                "id": fact.shift_record_id,
                "date": row[5].isoformat() if row[5] else None,
                "fullName": row[1],
                "clientName": row[2],
                "location": row[3],
                "site": row[4],
                "shiftName": row[6],
                "shiftStart": row[7].isoformat() if row[7] else None,
                "shiftEnd": row[8].isoformat() if row[8] else None,
                "paidHours": float(fact.paid_hours or 0),
                "totalPay": float(fact.total_pay or 0),
                "clientNet": float(fact.client_net or 0),
                "totalCharge": float(fact.total_charge or 0),
                "hourRate": float(fact.hour_rate or 0),
                "clientHourlyRate": float(fact.client_hourly_rate or 0),
                "jobStatus": fact.job_status,
                "jobName": fact.job.job_name if fact.job else "" # Access via relationship if needed or add to join
            }
            data.append(item)
            
        return jsonify({
            "data": data,
            "meta": {
                "page": pagination.page,
                "per_page": pagination.per_page,
                "total": pagination.total,
                "pages": pagination.pages
            }
        })
        
    except Exception as e:
        current_app.logger.error(f"Error fetching records: {e}")
        return jsonify({"error": str(e)}), 500
