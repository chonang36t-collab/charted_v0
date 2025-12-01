import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app import create_app
from app.models import db
from sqlalchemy import text

def create_performance_indexes():
    app = create_app()
    
    with app.app_context():
        print("🚀 CREATING PERFORMANCE INDEXES")
        print("=" * 40)
        
        indexes = [
            # Fact table indexes
            "CREATE INDEX IF NOT EXISTS idx_fact_employee_id ON fact_shifts(employee_id)",
            "CREATE INDEX IF NOT EXISTS idx_fact_client_id ON fact_shifts(client_id)",
            "CREATE INDEX IF NOT EXISTS idx_fact_job_id ON fact_shifts(job_id)",
            "CREATE INDEX IF NOT EXISTS idx_fact_date_id ON fact_shifts(date_id)",
            "CREATE INDEX IF NOT EXISTS idx_fact_shift_id ON fact_shifts(shift_id)",
            
            # Dimension table indexes
            "CREATE INDEX IF NOT EXISTS idx_employee_name ON dim_employees(full_name)",
            "CREATE INDEX IF NOT EXISTS idx_client_name ON dim_clients(client_name)",
            "CREATE INDEX IF NOT EXISTS idx_job_name ON dim_jobs(job_name)",
            "CREATE INDEX IF NOT EXISTS idx_job_location ON dim_jobs(location)",
            "CREATE INDEX IF NOT EXISTS idx_date_date ON dim_dates(date)",
            "CREATE INDEX IF NOT EXISTS idx_date_month ON dim_dates(month)",
            
            # Composite indexes for common queries
            "CREATE INDEX IF NOT EXISTS idx_fact_dates_client ON fact_shifts(date_id, client_id)",
            "CREATE INDEX IF NOT EXISTS idx_fact_dates_employee ON fact_shifts(date_id, employee_id)",
        ]
        
        for index_sql in indexes:
            try:
                db.session.execute(text(index_sql))
                print(f"✅ Created index: {index_sql.split('ON ')[1].split('(')[0]}")
            except Exception as e:
                print(f"❌ Failed to create index: {e}")
        
        db.session.commit()
        print("🎉 All indexes created successfully!")

if __name__ == "__main__":
    create_performance_indexes()