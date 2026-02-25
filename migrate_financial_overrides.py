"""
Migration: Create financial_summary_overrides table.
Run once: python migrate_financial_overrides.py
"""
from app import create_app
from app.models import db, FinancialSummaryOverride
from sqlalchemy import inspect


def migrate():
    app = create_app()
    with app.app_context():
        inspector = inspect(db.engine)
        if inspector.has_table('financial_summary_overrides'):
            print("✓ Table 'financial_summary_overrides' already exists — nothing to do.")
        else:
            print("Creating 'financial_summary_overrides' table...")
            db.create_all()
            print("✓ Table created successfully.")


if __name__ == "__main__":
    migrate()
