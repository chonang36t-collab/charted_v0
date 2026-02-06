"""
Migration script to add location and site columns to financial_metrics table.
Works with both SQLite and PostgreSQL.
"""
from app import create_app
from app.models import db
from sqlalchemy import text, inspect

def migrate():
    print("Starting migration: Add location/site to financial_metrics")
    
    app = create_app()
    
    with app.app_context():
        inspector = inspect(db.engine)
        
        # Check if table exists
        if not inspector.has_table('financial_metrics'):
            print("Creating financial_metrics table...")
            # Let SQLAlchemy create the table from the model
            db.create_all()
            print("Table created successfully.")
            return
        
        # Get existing columns
        columns = [col['name'] for col in inspector.get_columns('financial_metrics')]
        print(f"Existing columns: {columns}")
        
        # Add location column if missing
        if 'location' not in columns:
            print("Adding 'location' column...")
            try:
                with db.engine.connect() as conn:
                    conn.execute(text('ALTER TABLE financial_metrics ADD COLUMN location VARCHAR(100)'))
                    conn.commit()
                print("✓ 'location' column added")
            except Exception as e:
                print(f"Error adding 'location' column: {e}")
        else:
            print("✓ 'location' column already exists")
        
        # Add site column if missing
        if 'site' not in columns:
            print("Adding 'site' column...")
            try:
                with db.engine.connect() as conn:
                    conn.execute(text('ALTER TABLE financial_metrics ADD COLUMN site VARCHAR(100)'))
                    conn.commit()
                print("✓ 'site' column added")
            except Exception as e:
                print(f"Error adding 'site' column: {e}")
        else:
            print("✓ 'site' column already exists")
        
        print("\nMigration complete!")

if __name__ == "__main__":
    migrate()

