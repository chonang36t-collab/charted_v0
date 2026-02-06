#!/usr/bin/env bash
# Build script for Render

set -o errexit

# Install Python dependencies
pip install -r requirements.txt

# Run manual migration to add 2FA columns if missing
python migrate_change_2fa.py

# Run manual migration to add location/site columns to financial_metrics
python migrate_financials.py

# Initialize database and create admin user
python -c "from app import create_app, db; from create_admin import create_admin_user; app = create_app(); app.app_context().push(); db.create_all(); create_admin_user(); print('Database initialized and admin checked')"
