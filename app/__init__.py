"""Flask application factory for Sales Insight.

Loads configuration from .env, initializes extensions (SQLAlchemy, LoginManager),
registers blueprints, and ensures the instance folder exists for SQLite DB.
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Optional

from dotenv import load_dotenv
from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager

# Extensions

db = SQLAlchemy()
login_manager = LoginManager()
login_manager.login_view = "auth.login"
login_manager.login_message_category = "warning"


def create_app(test_config: Optional[dict] = None) -> Flask:
    """Application factory."""
    # Load environment variables
    # Project root is the parent of the app/ directory
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
    # Prefer DATABASE_URL from .env, fallback to instance sqlite
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        db_path = Path(app.instance_path) / "sales_data.db"
        database_url = f"sqlite:///{db_path}"
    else:
        # If sqlite path is relative (e.g., sqlite:///instance/sales_data.db),
        # convert to absolute to avoid OS-dependent resolution issues.
        if database_url.startswith("sqlite///") or database_url.startswith("sqlite:///"):
            prefix = "sqlite:///"
            if database_url.startswith("sqlite///"):
                prefix = "sqlite///"  # keep as-is; just normalize path below
            path_part = database_url[len("sqlite:///"):]
            if not os.path.isabs(path_part):
                project_root = Path(__file__).resolve().parents[1]
                abs_path = os.path.abspath(project_root / path_part)
                # Normalize to forward slashes for SQLAlchemy URI
                database_url = f"sqlite:///{abs_path.replace(os.sep, '/')}"

    app.config.from_mapping(
        SECRET_KEY=secret_key,
        SQLALCHEMY_DATABASE_URI=database_url,
        SQLALCHEMY_TRACK_MODIFICATIONS=False,
        UPLOAD_MAX_CONTENT_LENGTH=25 * 1024 * 1024,  # 25 MB
        ALLOWED_EXTENSIONS={"xlsx"},
    )

    if test_config is not None:
        app.config.update(test_config)

    # Ensure instance folder exists
    os.makedirs(app.instance_path, exist_ok=True)

    # Init extensions
    db.init_app(app)
    login_manager.init_app(app)

    # Import models so they are registered
    from . import models  # noqa: F401

    # Blueprints
    from .auth import auth_bp
    from .routes import main_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(main_bp)

    return app
