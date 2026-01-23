from app import create_app, db
from sqlalchemy import text

def add_column_if_not_exists(connection, table, column, column_type, default=None):
    # Check if column exists
    check_query = text(f"SELECT column_name FROM information_schema.columns WHERE table_name='{table}' AND column_name='{column}'")
    result = connection.execute(check_query).fetchone()
    
    if not result:
        print(f"Adding column {column} to {table}...")
        alter_query = f"ALTER TABLE {table} ADD COLUMN {column} {column_type}"
        if default is not None:
            alter_query += f" DEFAULT {default}"
        connection.execute(text(alter_query))
        print(f"Column {column} added.")
    else:
        print(f"Column {column} already exists in {table}.")

def migrate():
    app = create_app()
    with app.app_context():
        with db.engine.connect() as connection:
            transaction = connection.begin()
            try:
                # Add 2FA columns to users table
                add_column_if_not_exists(connection, 'users', 'otp_secret', 'VARCHAR(32)')
                add_column_if_not_exists(connection, 'users', 'two_factor_enabled', 'BOOLEAN', 'TRUE')
                add_column_if_not_exists(connection, 'users', 'two_factor_setup_complete', 'BOOLEAN', 'FALSE')
                
                transaction.commit()
                print("Migration complete successfully.")
            except Exception as e:
                transaction.rollback()
                print(f"Migration failed: {e}")

if __name__ == "__main__":
    migrate()
