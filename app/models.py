from flask_sqlalchemy import SQLAlchemy
from flask_login import UserMixin
from werkzeug.security import generate_password_hash, check_password_hash
from datetime import datetime

db = SQLAlchemy()

class User(UserMixin, db.Model):
    __tablename__ = 'users'
    
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(255), nullable=False)
    role = db.Column(db.String(20), default='user')
    location = db.Column(db.String(200), nullable=True)  # For manager location assignment
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # 2FA fields
    otp_secret = db.Column(db.String(32), nullable=True)
    two_factor_enabled = db.Column(db.Boolean, default=True)
    two_factor_setup_complete = db.Column(db.Boolean, default=False)
    
    def __repr__(self):
        return f'<User {self.username}>'
    
    def set_password(self, password):
        """Create hashed password."""
        self.password_hash = generate_password_hash(password)
    
    def check_password(self, password):
        """Check hashed password."""
        return check_password_hash(self.password_hash, password)
    
    @property
    def is_admin(self):
        return self.role == 'admin'

class DimEmployee(db.Model):
    __tablename__ = 'dim_employees'
    
    employee_id = db.Column(db.Integer, primary_key=True)
    full_name = db.Column(db.String(200), nullable=False)
    role = db.Column(db.String(100))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Relationships
    shifts = db.relationship('FactShift', backref='employee', lazy=True)

class DimClient(db.Model):
    __tablename__ = 'dim_clients'
    
    client_id = db.Column(db.Integer, primary_key=True)
    client_name = db.Column(db.String(200), nullable=False, unique=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Relationships
    shifts = db.relationship('FactShift', backref='client', lazy=True)

class DimJob(db.Model):
    __tablename__ = 'dim_jobs'
    
    job_id = db.Column(db.Integer, primary_key=True)
    job_name = db.Column(db.String(200), nullable=False)
    location = db.Column(db.String(200))
    site = db.Column(db.String(200))
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Relationships
    shifts = db.relationship('FactShift', backref='job', lazy=True)

class DimShift(db.Model):
    __tablename__ = 'dim_shifts'
    
    shift_id = db.Column(db.Integer, primary_key=True)
    shift_name = db.Column(db.String(200), nullable=False)
    shift_start = db.Column(db.Time)
    shift_end = db.Column(db.Time)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Relationships
    shifts = db.relationship('FactShift', backref='shift', lazy=True)

class DimDate(db.Model):
    __tablename__ = 'dim_dates'
    
    date_id = db.Column(db.Integer, primary_key=True)
    date = db.Column(db.String(10), nullable=False, unique=True)
    day = db.Column(db.String(20))
    month = db.Column(db.String(20))
    year = db.Column(db.Integer)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    # Relationships
    shifts = db.relationship('FactShift', backref='date', lazy=True)

class FactShift(db.Model):
    __tablename__ = 'fact_shifts'
    
    shift_record_id = db.Column(db.Integer, primary_key=True)
    
    # Foreign Keys
    employee_id = db.Column(db.Integer, db.ForeignKey('dim_employees.employee_id'))
    client_id = db.Column(db.Integer, db.ForeignKey('dim_clients.client_id'))
    job_id = db.Column(db.Integer, db.ForeignKey('dim_jobs.job_id'))
    shift_id = db.Column(db.Integer, db.ForeignKey('dim_shifts.shift_id'))
    date_id = db.Column(db.Integer, db.ForeignKey('dim_dates.date_id'))
    
    # Metrics
    duration = db.Column(db.Float)
    paid_hours = db.Column(db.Float)
    hour_rate = db.Column(db.Float)
    deductions = db.Column(db.Float, default=0)
    additions = db.Column(db.Float, default=0)
    total_pay = db.Column(db.Float)
    client_hourly_rate = db.Column(db.Float)
    client_net = db.Column(db.Float)
    self_employed = db.Column(db.Boolean)
    dns = db.Column(db.Boolean, default=False)
    job_status = db.Column(db.String(50))
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class PayBandSettings(db.Model):
    __tablename__ = 'pay_band_settings'
    
    id = db.Column(db.Integer, primary_key=True)
    premium_threshold = db.Column(db.Float, default=14.0)  # £14+/hour
    standard_threshold = db.Column(db.Float, default=13.0)  # £13-14/hour
    basic_threshold = db.Column(db.Float, default=12.0)    # £12-13/hour
    updated_at = db.Column(db.DateTime, default=datetime.utcnow)
    
    def __repr__(self):
        return f'<PayBandSettings premium={self.premium_threshold}, standard={self.standard_threshold}, basic={self.basic_threshold}>'

class ShiftTarget(db.Model):
    __tablename__ = 'shift_targets'
    
    id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.Integer, nullable=False)
    month = db.Column(db.String(20), nullable=False) # e.g. "January" or "01" - let's stick to Full Name or whatever DimDate uses. DimDate uses Full Name.
    location = db.Column(db.String(200), nullable=False)
    site = db.Column(db.String(200), nullable=True) # If null, applies to whole location (metric aggregation logic needed)
    target_count = db.Column(db.Integer, default=0)
    
    # Composite unique constraint to prevent duplicates
    __table_args__ = (db.UniqueConstraint('year', 'month', 'location', 'site', name='_year_month_loc_site_uc'),)

    created_at = db.Column(db.DateTime, default=datetime.utcnow)

class FinancialMetric(db.Model):
    __tablename__ = 'financial_metrics'
    
    id = db.Column(db.Integer, primary_key=True)
    year = db.Column(db.Integer, nullable=False)
    month = db.Column(db.String(20), nullable=False)
    name = db.Column(db.String(100), nullable=False) # e.g., "Overheads", "Marketing Spend"
    value = db.Column(db.Float, default=0.0)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
