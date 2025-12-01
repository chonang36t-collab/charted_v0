import sys
import os
import json
from datetime import datetime

# Add app directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))

from app import create_app, db
from app.models import User

def check_specific_range():
    app = create_app()
    
    # Use request context for easier login
    from flask_login import login_user
    from app.routes import api_sales_summary
    
    # Range seen in logs
    start_date = "2025-09-01"
    end_date = "2025-11-30"
    
    print(f"Checking API for range: {start_date} to {end_date}")
    
    with app.test_request_context(f'/api/sales-summary?start={start_date}&end={end_date}'):
        with app.app_context():
            user = User.query.filter_by(role='admin').first()
            if user:
                login_user(user)
                response = api_sales_summary()
                data = json.loads(response.data)
                print(f"API Response:")
                print(f"  Total Revenue: £{data.get('totalRevenue', 0):,.2f}")
                print(f"  Total Shifts: {data.get('totalShifts', 0)}")
                print(f"  Monthly Data: {len(data.get('monthlyRevenue', []))} months")
                for m in data.get('monthlyRevenue', []):
                    print(f"    - {m['month']}: £{m['revenue']:,.2f}")
            else:
                print("No admin user found.")

if __name__ == "__main__":
    check_specific_range()
