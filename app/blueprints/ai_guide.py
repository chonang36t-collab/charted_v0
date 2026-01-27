"""AI Guide Blueprint - Routes for AI-powered assistance."""

import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from flask import Blueprint, request, jsonify, current_app
from flask_login import login_required, current_user
from werkzeug.exceptions import TooManyRequests

from app.services.openai_service import get_ai_service


ai_guide_bp = Blueprint('ai_guide', __name__)


# Simple in-memory rate limiting (per user per day)
# For production, consider using Redis or database
_rate_limit_store = {}
QUERIES_PER_DAY = 5


def check_rate_limit(user_id: int) -> bool:
    """
    Check if user has exceeded rate limit.
    
    Args:
        user_id: The user's ID
    
    Returns:
        True if within limit, False if exceeded
    """
    today = datetime.now().date()
    key = f"{user_id}:{today}"
    
    # Clean up old entries (older than 2 days)
    cutoff = (datetime.now() - timedelta(days=2)).date()
    keys_to_remove = [k for k in _rate_limit_store.keys() 
                      if datetime.strptime(k.split(':')[1], '%Y-%m-%d').date() < cutoff]
    for k in keys_to_remove:
        del _rate_limit_store[k]
    
    # Check current count
    current_count = _rate_limit_store.get(key, 0)
    if current_count >= QUERIES_PER_DAY:
        return False
    
    # Increment count
    _rate_limit_store[key] = current_count + 1
    return True


def get_remaining_queries(user_id: int) -> int:
    """Get number of remaining queries for today."""
    today = datetime.now().date()
    key = f"{user_id}:{today}"
    current_count = _rate_limit_store.get(key, 0)
    return max(0, QUERIES_PER_DAY - current_count)


@ai_guide_bp.route('/capabilities', methods=['GET'])
@login_required
def get_capabilities():
    """
    Get the product capability map, filtered by user role.
    
    Returns:
        JSON response with capability map
    """
    try:
        # Load capability map
        cap_path = Path(current_app.root_path) / 'capabilities.json'
        with open(cap_path, 'r') as f:
            capabilities = json.load(f)
        
        # Filter based on user role
        user_role = current_user.role if hasattr(current_user, 'role') else 'viewer'
        
        # If not admin, remove financial metrics
        if user_role != 'admin':
            if 'chartBuilder' in capabilities and 'metrics' in capabilities['chartBuilder']:
                if 'financial' in capabilities['chartBuilder']['metrics']:
                    del capabilities['chartBuilder']['metrics']['financial']
        
        return jsonify({
            "success": True,
            "capabilities": capabilities,
            "userRole": user_role
        })
        
    except Exception as e:
        current_app.logger.error(f"Error loading capabilities: {e}")
        return jsonify({
            "success": False,
            "error": "Failed to load capabilities"
        }), 500


@ai_guide_bp.route('/query', methods=['POST'])
@login_required
def process_query():
    """
    Process an AI guide query.
    
    Expected JSON body:
    {
        "query": "string",
        "context": {
            "currentPage": "string",
            "userRole": "string"
        }
    }
    
    Returns:
        JSON response with guidance or alternatives
    """
    try:
        # Check rate limit
        user_id = current_user.id if hasattr(current_user, 'id') else 0
        if not check_rate_limit(user_id):
            remaining = get_remaining_queries(user_id)
            return jsonify({
                "success": False,
                "error": "Rate limit exceeded",
                "message": f"You have reached your daily limit of {QUERIES_PER_DAY} queries. Remaining today: {remaining}",
                "remainingQueries": remaining,
                "resetTime": "midnight UTC"
            }), 429
        
        # Parse request
        data = request.get_json()
        if not data or 'query' not in data:
            return jsonify({
                "success": False,
                "error": "Missing 'query' in request body"
            }), 400
        
        user_query = data['query']
        context = data.get('context', {})
        
        # Add user role to context
        user_role = current_user.role if hasattr(current_user, 'role') else 'viewer'
        context['userRole'] = user_role
        context['userId'] = user_id
        
        # Load capability map
        cap_path = Path(current_app.root_path) / 'capabilities.json'
        with open(cap_path, 'r') as f:
            capabilities = json.load(f)
        
        # Filter capabilities by role
        if user_role != 'admin':
            if 'chartBuilder' in capabilities and 'metrics' in capabilities['chartBuilder']:
                if 'financial' in capabilities['chartBuilder']['metrics']:
                    del capabilities['chartBuilder']['metrics']['financial']
        
        # Query AI service
        ai_service = get_ai_service(provider='openai')
        response = ai_service.query(user_query, capabilities, context)
        
        # Add metadata to response
        response['success'] = True
        response['remainingQueries'] = get_remaining_queries(user_id)
        response['queriesPerDay'] = QUERIES_PER_DAY
        
        return jsonify(response)
        
    except Exception as e:
        current_app.logger.error(f"Error processing AI query: {e}", exc_info=True)
        return jsonify({
            "success": False,
            "error": "Failed to process query",
            "message": str(e)
        }), 500


@ai_guide_bp.route('/status', methods=['GET'])
@login_required
def get_status():
    """
    Get AI Guide status and user's query quota.
    
    Returns:
        JSON with status information
    """
    try:
        user_id = current_user.id if hasattr(current_user, 'id') else 0
        user_role = current_user.role if hasattr(current_user, 'role') else 'viewer'
        
        return jsonify({
            "success": True,
            "enabled": True,
            "remainingQueries": get_remaining_queries(user_id),
            "queriesPerDay": QUERIES_PER_DAY,
            "userRole": user_role,
            "aiModel": os.environ.get('OPENAI_MODEL', 'gpt-4o')
        })
        
    except Exception as e:
        current_app.logger.error(f"Error getting AI Guide status: {e}")
        return jsonify({
            "success": False,
            "error": "Failed to get status"
        }), 500
