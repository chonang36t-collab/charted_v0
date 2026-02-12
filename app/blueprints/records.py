from flask import Blueprint, request, jsonify, current_app
from flask_login import login_required, current_user
from sqlalchemy import or_, desc, asc, text
from app.models import db, FactShift, DimEmployee, DimClient, DimJob, DimDate, DimShift
from app.utils.filters import apply_dashboard_filters
from app.auth import manager_required, admin_required
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
            
            # Role-based masking
            is_admin = (current_user.role == 'admin')
            
            item = {
                "id": fact.shift_record_id,
                "date": row[5] if row[5] else None,
                "fullName": row[1],
                "clientName": row[2],
                "location": row[3],
                "site": row[4],
                "shiftName": row[6],
                "shiftStart": row[7].isoformat() if row[7] else None,
                "shiftEnd": row[8].isoformat() if row[8] else None,
                "paidHours": float(fact.paid_hours or 0),
                "totalPay": float(fact.total_pay or 0) if is_admin else None,
                "clientNet": float(fact.client_net or 0) if is_admin else None,
                "totalCharge": float((fact.client_hourly_rate or 0) * (fact.paid_hours or 0)) if is_admin else None,
                "hourRate": float(fact.hour_rate or 0) if is_admin else None,
                "clientHourlyRate": float(fact.client_hourly_rate or 0) if is_admin else None,
                "jobStatus": fact.job_status,
                "jobName": fact.job.job_name if fact.job else "" 
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

@records_bp.route("/api/records/options")
@login_required
def api_record_options():
    try:
        employees = db.session.query(DimEmployee.employee_id, DimEmployee.full_name).order_by(DimEmployee.full_name).all()
        clients = db.session.query(DimClient.client_id, DimClient.client_name).order_by(DimClient.client_name).all()
        
        # Jobs - apply role filters
        job_query = db.session.query(DimJob.job_id, DimJob.job_name, DimJob.location, DimJob.site).order_by(DimJob.job_name)
        job_query = apply_dashboard_filters(job_query)
        jobs = job_query.all()
        
        shifts = db.session.query(DimShift.shift_id, DimShift.shift_name, DimShift.shift_start, DimShift.shift_end).order_by(DimShift.shift_name).all()
        
        return jsonify({
            "employees": [{"id": e[0], "name": e[1]} for e in employees],
            "clients": [{"id": c[0], "name": c[1]} for c in clients],
            "jobs": [{"id": j[0], "name": j[1], "location": j[2], "site": j[3]} for j in jobs],
            "shifts": [{"id": s[0], "name": s[1], "start": s[2].isoformat() if s[2] else "", "end": s[3].isoformat() if s[3] else ""} for s in shifts]
        })
    except Exception as e:
        current_app.logger.error(f"Error fetching options: {e}")
        return jsonify({"error": str(e)}), 500

@records_bp.route("/api/records", methods=["POST"])
@login_required
@manager_required
def create_record():
    try:
        data = request.json
        # Validate required fields
        req_fields = ['date', 'employeeId', 'clientId', 'jobId', 'shiftId', 'hours']
        for f in req_fields:
            if f not in data:
                 return jsonify({"error": f"Missing field: {f}"}), 400
                 
        # Date handling
        date_str = data['date'] # YYYY-MM-DD
        date_obj = DimDate.query.filter_by(date=date_str).first()
        if not date_obj:
            try:
                date_dt = datetime.strptime(date_str, '%Y-%m-%d')
                date_obj = DimDate(
                    date=date_str,
                    day=str(date_dt.day),
                    month=date_dt.strftime('%B'),
                    year=date_dt.year
                )
                db.session.add(date_obj)
                db.session.flush()
            except ValueError:
                return jsonify({"error": "Invalid date format"}), 400
            
        # Validate types (e.g., ensure IDs are ints not None or empty strings)
        try:
             employee_id = int(data['employeeId'])
             client_id = int(data['clientId'])
             job_id = int(data['jobId'])
             shift_id = int(data['shiftId'])
             paid_hours = float(data['hours'])
        except (ValueError, TypeError) as e:
             return jsonify({"error": f"Invalid data format for IDs or Hours: {str(e)}"}), 400

        pay_rate = 0.0
        charge_rate = 0.0
        
        is_admin = current_user.role == 'admin'
        if is_admin:
             try:
                 pay_rate = float(data.get('hourRate', 0) or 0)
                 charge_rate = float(data.get('clientHourlyRate', 0) or 0)
             except ValueError:
                 pass # Warning: defaults to 0 if invalid
        
        total_pay = pay_rate * paid_hours
        client_net = charge_rate * paid_hours
        
        new_record = FactShift(
            date_id=date_obj.date_id,
            employee_id=employee_id,
            client_id=client_id,
            job_id=job_id,
            shift_id=shift_id,
            paid_hours=paid_hours,
            hour_rate=pay_rate,
            client_hourly_rate=charge_rate,
            total_pay=total_pay,
            client_net=client_net,
            job_status=data.get('jobStatus', 'Completed')
        )
        
        db.session.add(new_record)
        db.session.commit()
        
        return jsonify({"message": "Record created", "id": new_record.shift_record_id}), 201
        
    except Exception as e:
        db.session.rollback()
        import traceback
        current_app.logger.error(f"Create error: {e}\n{traceback.format_exc()}")
        return jsonify({"error": f"Internal Error: {str(e)}"}), 500

@records_bp.route("/api/records/<int:id>", methods=["PUT"])
@login_required
@manager_required
def update_record(id):
    try:
        record = FactShift.query.get(id)
        if not record:
            return jsonify({"error": "Record not found"}), 404
            
        data = request.json
        is_admin = current_user.role == 'admin'
        
        if 'date' in data:
            date_str = data['date']
            date_obj = DimDate.query.filter_by(date=date_str).first()
            if not date_obj:
                 # Should theoretically create if missing, reusing logic
                 try:
                    date_dt = datetime.strptime(date_str, '%Y-%m-%d')
                    date_obj = DimDate(
                        date=date_str,
                        day=str(date_dt.day),
                        month=date_dt.strftime('%B'),
                        year=date_dt.year
                    )
                    db.session.add(date_obj)
                    db.session.flush()
                 except:
                    pass
            if date_obj:
                record.date_id = date_obj.date_id

        if 'employeeId' in data: record.employee_id = data['employeeId']
        if 'clientId' in data: record.client_id = data['clientId']
        if 'jobId' in data: record.job_id = data['jobId']
        if 'shiftId' in data: record.shift_id = data['shiftId']
        if 'jobStatus' in data: record.job_status = data['jobStatus']
        
        if 'hours' in data:
            record.paid_hours = float(data['hours'])
            record.total_pay = (record.hour_rate or 0) * record.paid_hours
            record.client_net = (record.client_hourly_rate or 0) * record.paid_hours
            
        if is_admin:
            if 'hourRate' in data:
                record.hour_rate = float(data['hourRate'])
                record.total_pay = record.hour_rate * (record.paid_hours or 0)
            if 'clientHourlyRate' in data:
                record.client_hourly_rate = float(data['clientHourlyRate'])
                record.client_net = record.client_hourly_rate * (record.paid_hours or 0)
                
        db.session.commit()
        return jsonify({"message": "Record updated"}), 200
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Update error: {e}")
        return jsonify({"error": str(e)}), 500

@records_bp.route("/api/records/<int:id>", methods=["DELETE"])
@login_required
@manager_required
def delete_record(id):
    try:
        record = FactShift.query.get(id)
        if not record:
            return jsonify({"error": "Record not found"}), 404
        
        db.session.delete(record)
        db.session.commit()
        return jsonify({"message": "Record deleted"}), 200
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Delete error: {e}")
        return jsonify({"error": str(e)}), 500

@records_bp.route("/api/records/bulk", methods=["DELETE"])
@login_required
@manager_required
def delete_records_bulk():
    """
    Delete multiple shift records in bulk.
    RBAC: Validates user can only delete records from their assigned locations.
    
    Request body: {"record_ids": [1, 2, 3, ...]}
    Response: {"deleted": count}
    """
    try:
        data = request.get_json()
        record_ids = data.get('record_ids', [])
        
        if not record_ids or not isinstance(record_ids, list):
            return jsonify({"error": "Invalid record_ids parameter"}), 400
        
        # Fetch records with their job locations
        records = db.session.query(FactShift, DimJob.location).join(
            DimJob, FactShift.job_id == DimJob.job_id
        ).filter(
            FactShift.shift_record_id.in_(record_ids)
        ).all()
        
        if not records:
            return jsonify({"error": "No records found"}), 404
        
        # RBAC: Filter by user's locations if not admin
        if current_user.role != 'admin':
            try:
                import json
                user_locations = json.loads(current_user.location) if current_user.location else []
                
                # Validate all records are from user's locations
                for record, location in records:
                    if location and location not in user_locations:
                        return jsonify({"error": "Unauthorized to delete some records"}), 403
            except Exception as e:
                current_app.logger.error(f"RBAC validation error: {e}")
                return jsonify({"error": "Permission validation failed"}), 500
        
        # Delete records
        deleted_count = 0
        for record, _ in records:
            db.session.delete(record)
            deleted_count += 1
        
        db.session.commit()
        
        return jsonify({"deleted": deleted_count}), 200
        
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"Bulk delete error: {e}")
        return jsonify({"error": str(e)}), 500
