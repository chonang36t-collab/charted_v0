"""Temporary diagnostic routes to check database state."""
from flask import Blueprint, jsonify
from sqlalchemy import func, distinct
from app import db
from app.models import FactShift, ShiftTarget, DimClient, DimDate, DimJob

diagnostic_bp = Blueprint("diagnostic", __name__)

@diagnostic_bp.route("/api/diagnostic/check")
def check_database():
    """Check database for client and target data - NO AUTH for quick diagnosis."""
    try:
        # 1. Check FactShift for client data
        total_shifts = db.session.query(func.count(FactShift.shift_record_id)).scalar() or 0
        shifts_with_clients = db.session.query(func.count(FactShift.shift_record_id)).filter(
            FactShift.client_id.isnot(None)
        ).scalar() or 0
        unique_clients = db.session.query(func.count(distinct(FactShift.client_id))).filter(
            FactShift.client_id.isnot(None)
        ).scalar() or 0
        
        # Check dim_client
        total_dim_clients = db.session.query(func.count(DimClient.client_id)).scalar() or 0
        
        # Date range with clients
        client_date_range = None
        if shifts_with_clients > 0:
            date_range = db.session.query(
                func.min(DimDate.date).label('min_date'),
                func.max(DimDate.date).label('max_date')
            ).join(
                FactShift, FactShift.date_id == DimDate.date_id
            ).filter(
                FactShift.client_id.isnot(None)
            ).first()
            if date_range:
                client_date_range = {
                    'min': str(date_range.min_date),
                    'max': str(date_range.max_date)
                }
        
        # 2. Check ShiftTarget table
        total_targets = db.session.query(func.count(ShiftTarget.id)).scalar() or 0
        target_periods = []
        sample_targets = []
        
        if total_targets > 0:
            # Get unique periods
            periods = db.session.query(
                ShiftTarget.year,
                ShiftTarget.month
            ).distinct().limit(10).all()
            target_periods = [{'year': y, 'month': m} for y, m in periods]
            
            # Sample targets
            samples = db.session.query(ShiftTarget).limit(5).all()
            for t in samples:
                sample_targets.append({
                    'year': t.year,
                    'month': t.month,
                    'location': t.location,
                    'site': t.site,
                    'target': t.target_count
                })
        
        # 3. Recent periods with shift data
        recent_periods = db.session.query(
            DimDate.year,
            DimDate.month,
            func.count(FactShift.shift_record_id).label('shift_count'),
            func.count(distinct(FactShift.client_id)).label('unique_clients')
        ).join(
            FactShift, FactShift.date_id == DimDate.date_id
        ).group_by(
            DimDate.year, DimDate.month
        ).order_by(
            DimDate.year.desc(), DimDate.month.desc()
        ).limit(6).all()
        
        period_data = []
        for year, month, shift_count, unique_clients_count in recent_periods:
            period_data.append({
                'year': year,
                'month': month,
                'shifts': shift_count,
                'unique_clients': unique_clients_count or 0
            })
        
        # Prepare response
        response = {
            'fact_shift': {
                'total_shifts': total_shifts,
                'shifts_with_clients': shifts_with_clients,
                'unique_clients': unique_clients,
                'percentage_with_clients': round((shifts_with_clients / total_shifts * 100), 1) if total_shifts > 0 else 0,
                'client_date_range': client_date_range
            },
            'dim_client': {
                'total_clients': total_dim_clients
            },
            'shift_target': {
                'total_targets': total_targets,
                'target_periods': target_periods,
                'sample_targets': sample_targets
            },
            'recent_periods': period_data,
            'issues': [],
            'recommendations': []
        }
        
        # Add issues and recommendations
        if shifts_with_clients == 0:
            response['issues'].append('NO_CLIENT_DATA')
            response['recommendations'].append('Upload shift data with client associations')
        
        if total_targets == 0:
            response['issues'].append('NO_TARGETS_CONFIGURED')
            response['recommendations'].append('Configure shift targets in Admin Metrics page')
        
        if total_shifts == 0:
            response['issues'].append('NO_SHIFT_DATA')
            response['recommendations'].append('Upload shift data to the system')
        
        return jsonify(response)
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500
