"""Authentication and user management blueprint."""
from __future__ import annotations
from functools import wraps
from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from flask_login import login_user, logout_user, login_required, current_user

from . import db
from .models import User



auth_bp = Blueprint("auth", __name__, url_prefix="")

def admin_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        # Check if user is authenticated
        if not current_user.is_authenticated:
            flash('Please log in to access this page.', 'warning')
            return redirect(url_for('auth.login', next=request.url))

        if not getattr(current_user, 'role', None) == 'admin':
            flash('You do not have permission to access this page. Admin access required.', 'danger')
            return redirect(url_for('main.dashboard'))
        
        return f(*args, **kwargs)
    return wrapper

def manager_required(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not current_user.is_authenticated:
            flash('Please log in to access this page.', 'warning')
            return redirect(url_for('auth.login', next=request.url))
        
        # Allow both admin and manager roles
        if current_user.role not in ['admin', 'manager']:
            flash('Manager or Admin access required.', 'danger')
            return redirect(url_for('main.dashboard'))
        
        return f(*args, **kwargs)
    return wrapper


@auth_bp.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        username = request.form.get("username", "").strip()
        password = request.form.get("password", "")
        user = User.query.filter_by(username=username).first()
        if user and user.check_password(password):
            login_user(user)
            flash("Logged in successfully.", "success")
            next_url = request.args.get("next") or url_for("main.dashboard")
            return redirect(next_url)
        flash("Invalid username or password.", "danger")
    return render_template("login.html")


@auth_bp.route("/api/login", methods=["POST"])
def api_login():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""

    if not username or not password:
        return jsonify({"error": "Username and password are required."}), 400

    user = User.query.filter_by(username=username).first()
    if user and user.check_password(password):
        login_user(user)
        return jsonify({"token": "session", "message": "Login successful."})

    return jsonify({"error": "Invalid username or password."}), 401


@auth_bp.route("/logout")
@login_required
def logout():
    logout_user()
    flash("Logged out.", "info")
    return redirect(url_for("auth.login"))


@auth_bp.route("/api/users", methods=["GET"])
@login_required
@admin_required
def api_list_users():
    users = User.query.order_by(User.created_at.desc()).all()
    return jsonify([
        {
            "id": user.id,
            "username": user.username,
            "role": user.role,
            "created_at": user.created_at.isoformat() if user.created_at else None,
        }
        for user in users
    ])


@auth_bp.route("/api/users", methods=["POST"])
@login_required
@admin_required
def api_create_user():
    data = request.get_json(silent=True) or {}
    username = (data.get("username") or "").strip()
    password = data.get("password") or ""
    role = data.get("role") or "viewer"

    if not username or not password:
        return jsonify({"error": "Username and password are required."}), 400

    if User.query.filter_by(username=username).first():
        return jsonify({"error": "Username already exists."}), 409

    user = User(username=username, role=role)
    user.set_password(password)
    db.session.add(user)
    db.session.commit()
    return jsonify({
        "message": "User created.",
        "user": {
            "id": user.id,
            "username": user.username,
            "role": user.role,
        },
    }), 201
