from flask import request
from flask_login import current_user
from sqlalchemy import text, or_
from app.models import DimJob, DimClient, FactShift, DimEmployee

def apply_dashboard_filters(query):
    """
    Unified utility to apply location and site filtering based on user role 
    and requested filters (from query parameters).
    """
    requested_clients = request.args.getlist("clients")
    requested_locations = request.args.getlist("locations")
    requested_sites = request.args.getlist("sites")
    
    # helper to safely join DimJob only if not already present
    def ensure_dim_job(q):
        # Check current joins to avoid DuplicateAlias
        is_joined = False
        if hasattr(q, '_setup_joins'):
            for join in q._setup_joins:
                target = join[0]
                if hasattr(target, 'class_') and target.class_ == DimJob:
                    is_joined = True
                    break
                if target == DimJob:
                    is_joined = True
                    break
                    
        # Also check column_descriptions if it was selected in the FROM
        if not is_joined:
            for desc in q.column_descriptions:
                entity = desc['entity']
                if entity == DimJob:
                    is_joined = True
                    break
                    
        if not is_joined:
            return q.join(DimJob, FactShift.job_id == DimJob.job_id)
        return q

    # helper safely join DimClient
    def ensure_dim_client(q):
        is_joined = False
        if hasattr(q, '_setup_joins'):
            for join in q._setup_joins:
                target = join[0]
                if hasattr(target, 'class_') and target.class_ == DimClient:
                    is_joined = True
                    break
                if target == DimClient:
                    is_joined = True
                    break
        
        if not is_joined:
             for desc in q.column_descriptions:
                entity = desc['entity']
                if entity == DimClient:
                    is_joined = True
                    break
                    
        if not is_joined:
            return q.join(DimClient, FactShift.client_id == DimClient.client_id)
        return q

    # 0. Handle Client Filtering
    if requested_clients:
        query = ensure_dim_client(query)
        query = query.filter(DimClient.client_name.in_(requested_clients))

    # Determine if we need to filter on DimJob (Location/Site)
    # We need DimJob if:
    # 1. User is NOT admin (needs location security)
    # 2. Locations are requested
    # 3. Sites are requested
    need_dim_job = (current_user.role != 'admin') or requested_locations or requested_sites
    
    if need_dim_job:
        query = ensure_dim_job(query)

    # 1. Handle Role-Based & Requested Location Filtering
    if current_user.role == 'admin':
        if requested_locations:
             query = query.filter(DimJob.location.in_(requested_locations))
    else:
        # Non-admins: Security intersection
        import json
        user_locations = json.loads(current_user.location) if current_user.location else []
        
        if not user_locations:
            # If manager has no locations, they see nothing
            return query.filter(text("1=0"))
        
        if requested_locations:
            # Intersection of Requested AND Assigned
            target_locations = []
            for req in requested_locations:
                if any(u in req or req in u for u in user_locations):
                    target_locations.append(req)
            
            if not target_locations:
                 return query.filter(text("1=0"))
            query = query.filter(DimJob.location.in_(target_locations))
        else:
            # Default: show all user locations
            filters = [DimJob.location.like(f"%{loc}%") for loc in user_locations]
            query = query.filter(or_(*filters))

    # 2. Handle Site Filtering
    if requested_sites:
        query = query.filter(DimJob.site.in_(requested_sites))
        
    return query
