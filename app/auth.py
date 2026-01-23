"""Authentication and user management blueprint."""
from __future__ import annotations
from functools import wraps
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify, session
from flask_login import login_user, logout_user, login_required, current_user

from . import db
from .models import User

import pyotp
import qrcode
import io
import base64
import json

auth_bp = Blueprint("auth", __name__, url_prefix="")

def admin_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not current_user.is_authenticated:
            flash('Please log in to access this page.', 'warning')
            return redirect('/auth?next=' + request.url)

        if not getattr(current_user, 'role', None) == 'admin':
            flash('You do not have permission to access this page. Admin access required.', 'danger')
            return redirect('/')
        
        return f(*args, **kwargs)
    return wrapper

def manager_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not current_user.is_authenticated:
            flash('Please log in to access this page.', 'warning')
            return redirect('/auth?next=' + request.url)
        
        if current_user.role not in ['admin', 'manager']:
            flash('Manager or Admin access required.', 'danger')
            return redirect('/')
        
        return f(*args, **kwargs)
    return wrapper

@auth_bp.route("/auth-redirect")
def login_redirect():
    next_url = request.args.get("next")
    return redirect("/auth")


@auth_bp.route("/logout")
@login_required
def logout():
    logout_user()
    session.pop('pending_user_id', None) # Clean up
    flash("Logged out.", "info")
    return redirect("/auth")


@auth_bp.route("/api/login", methods=["POST"])
def api_login():
    data = request.get_json(silent=True) or {}
    username_or_email = (data.get("username") or "").strip()
    password = data.get("password") or ""

    if not username_or_email or not password:
        return jsonify({"error": "Username/Email and password are required."}), 400

    # Try to find user by username or email
    user = User.query.filter(
        (User.username == username_or_email) | (User.email == username_or_email)
    ).first()
    
    if user and user.check_password(password):
        if not user.two_factor_enabled:
            login_user(user)
            return jsonify({"token": "session", "message": "Login successful."})
        
        # 2FA requested - Store in session
        session['pending_user_id'] = user.id
        
        if not user.two_factor_setup_complete:
            return jsonify({"status": "2fa_setup_required"})
        
        return jsonify({"status": "2fa_login_required"})

    return jsonify({"error": "Invalid username or password."}), 401


@auth_bp.route("/api/2fa/setup", methods=["POST"])
def api_2fa_setup():
    # Use session instead of request body for security
    user_id = session.get('pending_user_id')
    if not user_id:
         return jsonify({"error": "Unauthorized: No pending login session."}), 401

    user = User.query.get(user_id)
    if not user:
        return jsonify({"error": "User not found"}), 404
    
    if not user.otp_secret:
        user.otp_secret = pyotp.random_base32()
        db.session.commit()
    
    totp = pyotp.TOTP(user.otp_secret)
    provisioning_uri = totp.provisioning_uri(name=user.email, issuer_name="Sales Insight")
    
    # Generate QR Code
    try:
        img = qrcode.make(provisioning_uri)
        buffered = io.BytesIO()
        img.save(buffered, format="PNG")
        img_str = base64.b64encode(buffered.getvalue()).decode()
    except Exception as e:
        return jsonify({"error": f"QR generation failed: {str(e)}"}), 500
    
    return jsonify({
        "qr_code": f"data:image/png;base64,{img_str}",
        "secret": user.otp_secret
    })


@auth_bp.route("/api/2fa/verify-setup", methods=["POST"])
def api_2fa_verify_setup():
    data = request.get_json(silent=True) or {}
    token = data.get("token")
    
    user_id = session.get('pending_user_id')
    if not user_id:
         return jsonify({"error": "Unauthorized: No pending login session."}), 401

    user = User.query.get(user_id)
    if not user or not user.otp_secret:
        return jsonify({"error": "Invalid request"}), 400
    
    totp = pyotp.TOTP(user.otp_secret)
    if totp.verify(token):
        user.two_factor_setup_complete = True
        db.session.commit()
        session.pop('pending_user_id', None) # Clear pending session
        login_user(user)
        return jsonify({"token": "session", "message": "2FA setup complete and logged in."})
    
    return jsonify({"error": "Invalid verification token"}), 401


@auth_bp.route("/api/debug/qr", methods=["GET"])
def debug_qr():
    try:
        import sys
        # Test qrcode generation
        img = qrcode.make("test_debug_qr")
        buffered = io.BytesIO()
        img.save(buffered, format="PNG")
        img_str = base64.b64encode(buffered.getvalue()).decode()
        
        # Check packages
        try:
            import PIL
            pil_version = PIL.__version__
            pil_file = PIL.__file__
        except ImportError:
            pil_version = "Not installed"
            pil_file = "N/A"
            
        return jsonify({
            "status": "success",
            "message": "QR Code generated successfully",
            "pil_version": pil_version,
            "pil_file": pil_file,
            "python_executable": sys.executable,
            "qr_sample_len": len(img_str)
        })
    except Exception as e:
        import traceback
        return jsonify({
            "status": "error",
            "error": str(e),
            "traceback": traceback.format_exc()
        }), 500


@auth_bp.route("/api/2fa/login-verify", methods=["POST"])
def api_2fa_login_verify():
    data = request.get_json(silent=True) or {}
    token = data.get("token")
    
    user_id = session.get('pending_user_id')
    if not user_id:
         return jsonify({"error": "Unauthorized: No pending login session."}), 401

    user = User.query.get(user_id)
    if not user or not user.otp_secret:
        return jsonify({"error": "Invalid request"}), 400
    
    totp = pyotp.TOTP(user.otp_secret)
    if totp.verify(token):
        login_user(user)
        session.pop('pending_user_id', None) # Clear pending session
        return jsonify({"token": "session", "message": "Login successful."})
    
    return jsonify({"error": "Invalid verification token"}), 401


@auth_bp.route("/api/user/profile", methods=["GET"])
@login_required
def api_get_current_user():
    """Get current logged-in user's profile"""
    import json
    return jsonify({
        "id": current_user.id,
        "username": current_user.username,
        "email": current_user.email,
        "role": current_user.role,
        "location": current_user.location,
        "locations": json.loads(current_user.location) if current_user.location else [],
    })


@auth_bp.route("/api/users", methods=["GET"])
@login_required
@admin_required
def api_list_users():
    import json
    users = User.query.order_by(User.created_at.desc()).all()
    return jsonify([
        {
            "id": user.id,
            "name": user.username,  # Return as 'name' for frontend compatibility
            "username": user.username,
            "email": user.email,
            "role": user.role,
            "location": user.location,
            "locations": json.loads(user.location) if user.location else [],  # Parse JSON array
            "two_factor_enabled": user.two_factor_enabled,
            "two_factor_setup_complete": user.two_factor_setup_complete,
            "status": "Active",  # Default status
            "created_at": user.created_at.isoformat() if user.created_at else None,
        }
        for user in users
    ])


@auth_bp.route("/api/users", methods=["POST"])
@login_required
@admin_required
def api_create_user():
    data = request.get_json(silent=True) or {}
    # Accept both 'name' and 'username' for compatibility
    username = (data.get("username") or data.get("name") or "").strip()
    email = (data.get("email") or "").strip()
    password = data.get("password") or ""
    role = data.get("role") or "viewer"
    locations = data.get("locations") or data.get("location")  # Support both array and single value
    
    # Convert single location to array, or use array as-is
    if isinstance(locations, str):
        locations = [locations] if locations else []
    elif not isinstance(locations, list):
        locations = []
    
    # Store as JSON string
    import json
    location_json = json.dumps(locations) if locations else None

    if not username:
        return jsonify({"error": "Username is required."}), 400
    
    if not password:
        return jsonify({"error": "Password is required."}), 400
    
    if not email:
        return jsonify({"error": "Email is required."}), 400

    two_factor_enabled = data.get("two_factor_enabled", True)
    
    if User.query.filter_by(username=username).first():
        return jsonify({"error": "Username already exists."}), 409
    
    if User.query.filter_by(email=email).first():
        return jsonify({"error": "Email already exists."}), 409

    user = User(
        username=username, 
        email=email, 
        role=role, 
        location=location_json,
        two_factor_enabled=two_factor_enabled
    )
    user.set_password(password)
    db.session.add(user)
    db.session.commit()
    return jsonify({
        "message": "User created.",
        "user": {
            "id": user.id,
            "name": user.username,  # Return as 'name' for frontend
            "username": user.username,
            "email": user.email,
            "role": user.role,
            "location": user.location,
            "locations": locations,  # Return parsed array
        },
    }), 201


@auth_bp.route('/api/users/<int:user_id>', methods=['PUT'])
@login_required
@admin_required
def api_update_user(user_id):
    user = User.query.get_or_404(user_id)
    data = request.get_json(silent=True) or {}
    
    # Accept both 'name' and 'username' for compatibility
    username = (data.get('username') or data.get('name') or '').strip()
    email = (data.get('email') or '').strip()
    role = data.get('role')
    locations = data.get('locations') or data.get('location')  # Support both array and single value
    password = data.get('password')
    
    # Convert single location to array, or use array as-is
    if isinstance(locations, str):
        locations = [locations] if locations else []
    elif not isinstance(locations, list):
        locations = []
    
    # Store as JSON string
    import json
    location_json = json.dumps(locations) if locations else None
    
    if username and username != user.username:
        if User.query.filter_by(username=username).first():
            return jsonify({'error': 'Username already exists.'}), 409
        user.username = username
    
    if email and email != user.email:
        if User.query.filter_by(email=email).first():
            return jsonify({'error': 'Email already exists.'}), 409
        user.email = email
    
    if role:
        user.role = role
    
    # Update location
    user.location = location_json
    
    if "two_factor_enabled" in data:
        user.two_factor_enabled = bool(data["two_factor_enabled"])
        if not user.two_factor_enabled:
            # Reset setup if disabled? Or keep secret?
            # Keeping secret is safer for re-enabling, but maybe we want a fresh start
             user.two_factor_setup_complete = False
             user.otp_secret = None
    
    if password:
        user.set_password(password)
    
    db.session.commit()
    
    return jsonify({
        'message': 'User updated successfully.',
        'user': {
            'id': user.id,
            'name': user.username,
            'username': user.username,
            'email': user.email,
            'role': user.role,
            'location': user.location,
            'locations': locations,
            'status': 'Active',
        },
    })


@auth_bp.route('/api/users/<int:user_id>', methods=['DELETE'])
@login_required
@admin_required
def api_delete_user(user_id):
    # Prevent users from deleting themselves
    if user_id == current_user.id:
        return jsonify({
            'error': 'You cannot delete your own account.'
        }), 400  # 400 Bad Request is more appropriate than 403 Forbidden here
    
    user = User.query.get_or_404(user_id)
    db.session.delete(user)
    db.session.commit()
    return jsonify({
        'message': 'User deleted successfully.'
    }), 200

@auth_bp.route('/api/locations', methods=['GET'])
@login_required
def api_list_locations():
    from .models import DimJob
    
    locations = db.session.query(DimJob.location).distinct().filter(
        DimJob.location.isnot(None),
        DimJob.location != ''
    ).order_by(DimJob.location).all()
    
    location_list = [loc[0] for loc in locations]
    
    return jsonify(location_list)
