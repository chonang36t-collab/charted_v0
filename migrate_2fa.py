from app import create_app, db
from sqlalchemy import text

def migrate():
    app = create_app()
    with app.app_context():
        print("Starting 2FA database migration...")
        
        inspector = db.inspect(db.engine)
        user_columns = [col['name'] for col in inspector.get_columns('users')]
        
        added = False
        if 'otp_secret' not in user_columns:
            print("Adding 'otp_secret' column...")
            with db.engine.connect() as conn:
                conn.execute(text("ALTER TABLE users ADD COLUMN otp_secret VARCHAR(32)"))
                conn.commit()
            print("✓ Added otp_secret")
            added = True
            
        if 'two_factor_enabled' not in user_columns:
            print("Adding 'two_factor_enabled' column...")
            with db.engine.connect() as conn:
                conn.execute(text("ALTER TABLE users ADD COLUMN two_factor_enabled BOOLEAN DEFAULT TRUE"))
                conn.execute(text("UPDATE users SET two_factor_enabled = TRUE"))
                conn.commit()
            print("✓ Added two_factor_enabled")
            added = True

        if 'two_factor_setup_complete' not in user_columns:
            print("Adding 'two_factor_setup_complete' column...")
            with db.engine.connect() as conn:
                conn.execute(text("ALTER TABLE users ADD COLUMN two_factor_setup_complete BOOLEAN DEFAULT FALSE"))
                conn.execute(text("UPDATE users SET two_factor_setup_complete = FALSE"))
                conn.commit()
            print("✓ Added two_factor_setup_complete")
            added = True
            
        if not added:
            print("✓ 2FA columns already exist")
        
        print("\n✅ 2FA Migration completed successfully!")

if __name__ == "__main__":
    migrate()
