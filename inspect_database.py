import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app import create_app
from app.models import db

def diagnose_database():
    app = create_app()
    
    with app.app_context():
        print("🔍 DATABASE DIAGNOSIS")
        print("=" * 50)
        
        # 1. Check database connection
        try:
            from sqlalchemy import text
            result = db.session.execute(text('SELECT version()'))
            version = result.scalar()
            print(f"✅ PostgreSQL Connection: {version.split(',')[0]}")
        except Exception as e:
            print(f"❌ Database connection failed: {e}")
            return
        
        # 2. Check if tables exist
        try:
            inspector = db.inspect(db.engine)
            tables = inspector.get_table_names()
            print(f"✅ Tables in database: {len(tables)}")
            for table in sorted(tables):
                print(f"   - {table}")
        except Exception as e:
            print(f"❌ Could not list tables: {e}")
            return
        
        # 3. Check if our models are registered
        try:
            from app.models import DimEmployee, DimClient, DimJob, FactShift
            print("✅ Model imports successful")
            
            # Try to query each model
            models = {
                'DimEmployee': DimEmployee,
                'DimClient': DimClient, 
                'DimJob': DimJob,
                'FactShift': FactShift
            }
            
            print("\n📊 MODEL QUERY TEST:")
            for name, model in models.items():
                try:
                    count = db.session.query(model).count()
                    print(f"   {name}: {count} records")
                except Exception as e:
                    print(f"   {name}: ❌ Query failed - {e}")
                    
        except ImportError as e:
            print(f"❌ Model import failed: {e}")
        except Exception as e:
            print(f"❌ Model query failed: {e}")
        
        # 4. Check database URL
        print(f"\n🔗 Database URL: {app.config.get('SQLALCHEMY_DATABASE_URI', 'Not set')}")

if __name__ == "__main__":
    diagnose_database()