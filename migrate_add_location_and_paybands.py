"""
Database migration script to add location field to users table and create pay_band_settings table.

Run this script to update the database schema:
    python migrate_add_location_and_paybands.py
"""

from app import create_app, db
from app.models import User, PayBandSettings
from sqlalchemy import text

def migrate():
    app = create_app()
    with app.app_context():
        print("Starting database migration...")
        
        # Check if location column exists
        inspector = db.inspect(db.engine)
        user_columns = [col['name'] for col in inspector.get_columns('users')]
        
        if 'location' not in user_columns:
            print("Adding 'location' column to users table...")
            with db.engine.connect() as conn:
                conn.execute(text("ALTER TABLE users ADD COLUMN location VARCHAR(200)"))
                conn.commit()
            print("✓ Added location column")
        else:
            print("✓ Location column already exists")
        
        # Create pay_band_settings table
        print("Creating pay_band_settings table...")
        db.create_all()
        print("✓ Created pay_band_settings table")
        
        # Initialize default pay band settings if not exists
        if PayBandSettings.query.first() is None:
            print("Initializing default pay band settings...")
            default_settings = PayBandSettings(
                premium_threshold=14.0,
                standard_threshold=13.0,
                basic_threshold=12.0
            )
            db.session.add(default_settings)
            db.session.commit()
            print("✓ Initialized default pay band settings (Premium: £14+, Standard: £13-14, Basic: £12-13)")
        else:
            print("✓ Pay band settings already exist")
        
        print("\n✅ Migration completed successfully!")

if __name__ == "__main__":
    migrate()
