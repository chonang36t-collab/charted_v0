import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app import create_app
from app.models import db
from sqlalchemy import text  # Add this import

def test_postgres():
    app = create_app()
    
    with app.app_context():
        try:
            # Test connection - use text() for raw SQL
            result = db.session.execute(text('SELECT version()'))
            version = result.scalar()
            print(f"✅ Connected to: {version}")
            
            # Test if tables exist
            tables = db.inspect(db.engine).get_table_names()
            print(f"✅ Tables in database: {len(tables)}")
            for table in tables:
                print(f"   - {table}")
                
        except Exception as e:
            print(f"❌ Connection failed: {e}")

if __name__ == "__main__":
    test_postgres()