import sys
import os
import json
from datetime import datetime

# Add app directory to path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__))))

from app import create_app, db
from app.models import User

def check_sort_order():
    app = create_app()
    
    # Use request context for easier login
    from flask_login import login_user
    from app.routes import api_sales_summary
    
    # "This year" range (approximate)
    start_date = "2025-01-01"
    end_date = "2025-12-31"
    
    print(f"Checking API sort order for range: {start_date} to {end_date}")
    
    with app.test_request_context(f'/api/sales-summary?start={start_date}&end={end_date}'):
        with app.app_context():
            user = User.query.filter_by(role='admin').first()
            if user:
                login_user(user)
                response = api_sales_summary()
                data = json.loads(response.data)
                monthly_data = data.get('monthlyRevenue', [])
                
                print(f"Returned {len(monthly_data)} months:")
                months = [m['month'] for m in monthly_data]
                print(" -> ".join(months))
                
                # Check if sorted
                month_map = {
                    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
                }
                
                indices = [month_map.get(m, 0) for m in months]
                if indices == sorted(indices):
                    print("\nSUCCESS: Data is correctly sorted.")
                else:
                    print("\nFAILURE: Data is NOT sorted.")
            else:
                print("No admin user found.")

if __name__ == "__main__":
    check_sort_order()
