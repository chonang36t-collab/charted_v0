import time
import requests
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app import create_app
from app.models import db
from sqlalchemy import text

def test_query_performance():
    app = create_app()
    
    with app.app_context():
        print("🔍 PERFORMANCE TESTING")
        print("=" * 50)
        
        # Test 1: Basic count query
        start = time.time()
        result = db.session.execute(text("SELECT COUNT(*) FROM fact_shifts"))
        count = result.scalar()
        end = time.time()
        print(f"✅ Basic count: {count} rows in {(end-start)*1000:.2f}ms")
        
        # Test 2: Revenue aggregation
        start = time.time()
        result = db.session.execute(text("""
            SELECT 
                SUM(client_net) as revenue,
                SUM(total_pay) as cost,
                COUNT(*) as shifts
            FROM fact_shifts
        """))
        data = result.fetchone()
        end = time.time()
        print(f"✅ Basic aggregation: {(end-start)*1000:.2f}ms")
        
        # Test 3: Date-filtered query
        start = time.time()
        result = db.session.execute(text("""
            SELECT 
                SUM(fs.client_net) as revenue,
                SUM(fs.total_pay) as cost,
                COUNT(*) as shifts
            FROM fact_shifts fs
            JOIN dim_dates dd ON fs.date_id = dd.date_id
            WHERE dd.date BETWEEN '2024-01-01' AND '2024-12-31'
        """))
        data = result.fetchone()
        end = time.time()
        print(f"✅ Date-filtered: {(end-start)*1000:.2f}ms")

if __name__ == "__main__":
    test_query_performance()