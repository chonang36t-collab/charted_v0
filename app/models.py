"""Database models for Sales Insight."""
from __future__ import annotations

from datetime import datetime

from flask_login import UserMixin
from sqlalchemy import UniqueConstraint
from werkzeug.security import generate_password_hash, check_password_hash

from . import db, login_manager


class User(db.Model, UserMixin):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(20), nullable=False, default="viewer")  # 'admin' or 'viewer'
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    def set_password(self, password: str) -> None:
        self.password_hash = generate_password_hash(password)

    def check_password(self, password: str) -> bool:
        return check_password_hash(self.password_hash, password)

    @property
    def is_admin(self) -> bool:
        return self.role == "admin"


@login_manager.user_loader
def load_user(user_id: str):
    return User.query.get(int(user_id))



class JobRecord(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    job_name = db.Column(db.String(255), nullable=False)
    shift_name = db.Column(db.String(255), nullable=True)
    full_name = db.Column(db.String(255), nullable=False)
    location = db.Column(db.String(255), nullable=True)
    site = db.Column(db.String(255), nullable=True)
    role = db.Column(db.String(128), nullable=True)
    month = db.Column(db.String(32), nullable=True)
    date = db.Column(db.Date, nullable=False)
    day = db.Column(db.String(32), nullable=True)
    shift_start = db.Column(db.String(64), nullable=True)
    shift_end = db.Column(db.String(64), nullable=True)
    duration = db.Column(db.Float, nullable=True, default=0.0)
    paid_hours = db.Column(db.Float, nullable=True, default=0.0)
    hour_rate = db.Column(db.Float, nullable=True, default=0.0)
    deductions = db.Column(db.Float, nullable=True, default=0.0)
    additions = db.Column(db.Float, nullable=True, default=0.0)
    total_pay = db.Column(db.Float, nullable=True, default=0.0)
    client_hourly_rate = db.Column(db.Float, nullable=True, default=0.0)
    client_net = db.Column(db.Float, nullable=True, default=0.0)
    self_employed = db.Column(db.Boolean, nullable=True, default=False)
    dns = db.Column(db.String(255), nullable=True)
    client = db.Column(db.String(255), nullable=True)
    job_status = db.Column(db.String(128), nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("date", "full_name", "job_name", name="uq_date_fullname_job"),
    )
