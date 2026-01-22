"""Flask application factory for Sales Insight."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from flask import Flask, send_from_directory
from flask_login import LoginManager
from flask_caching import Cache
from flask_cors import CORS

# Import db from models to avoid circular imports
from .models import db, User

# Initialize cache
cache = Cache()

login_manager = LoginManager()
login_manager.login_view = "/auth"
login_manager.login_message_category = "warning"

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))


def create_app(test_config: Optional[dict] = None) -> Flask:
    """Application factory."""
    # Load environment variables
    env_path = Path(__file__).resolve().parents[1] / ".env"
    if env_path.exists():
        load_dotenv(env_path)

    app = Flask(
        __name__,
        instance_path=str(Path(__file__).resolve().parents[1] / "instance"),
        instance_relative_config=True,
        static_folder="static",
        template_folder="templates",
    )

    # Default config
    secret_key = os.getenv("SECRET_KEY", os.urandom(24).hex())
    
    database_url = os.getenv("DATABASE_URL")
    
    if not database_url:
        # Fallback to PostgreSQL with default credentials
        database_url = "postgresql://postgres:admin123@localhost:5432/sales_insight"
        print("  Using default PostgreSQL connection. Set DATABASE_URL in .env for production.")

    app.config.from_mapping(
        SECRET_KEY=secret_key,
        SQLALCHEMY_DATABASE_URI=database_url,  # This now points to PostgreSQL
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
        UPLOAD_MAX_CONTENT_LENGTH=25 * 1024 * 1024,
        ALLOWED_EXTENSIONS={"xlsx"},
    )

    if test_config is not None:
        app.config.update(test_config)

    # Ensure instance folder exists
    os.makedirs(app.instance_path, exist_ok=True)

    # Init extensions
    db.init_app(app)
    login_manager.init_app(app)
    cache.init_app(app, config={
        'CACHE_TYPE': 'SimpleCache',
        'CACHE_DEFAULT_TIMEOUT': 300  # 5 minutes
    })
    CORS(app)

    # Import models so they are registered
    from . import models  # noqa: F401

    # Create tables
    with app.app_context():
        try:
            db.create_all()
            print(" PostgreSQL database tables created/verified")
        except Exception as e:
            print(f" Error creating tables: {e}")
            # Don't raise here to see the full error

    # Blueprints
    from .auth import auth_bp
    from .routes import main_bp
    from .blueprints.advanced_reports import advanced_reports_bp
    from .blueprints.upload import upload_bp
    from .blueprints.records import records_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(main_bp)
    app.register_blueprint(advanced_reports_bp)
    app.register_blueprint(upload_bp, url_prefix='/api')
    app.register_blueprint(records_bp)

    @app.route("/", defaults={"path": ""})
    @app.route("/<path:path>")
    def serve_react(path: str):
        file_path = os.path.join(app.static_folder, path)

        if path != "" and os.path.exists(file_path):
            return send_from_directory(app.static_folder, path)

        return send_from_directory(app.static_folder, "index.html")

    return app