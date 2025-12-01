from app import create_app
from app.models import db, FactShift, DimClient, DimDate

def check_data():
    app = create_app()
    with app.app_context():
        # Check counts of each table
        print("\n=== Table Counts ===")
        print(f"FactShift: {FactShift.query.count()}")
        print(f"DimClient: {DimClient.query.count()}")
        print(f"DimDate: {DimDate.query.count()}")
        
        # Check first few records if they exist
        if FactShift.query.count() > 0:
            print("\n=== Sample Data ===")
            shift = FactShift.query.first()
            print(f"First shift: {shift.shift_record_id}")
            print(f"Client: {shift.client.client_name if shift.client else 'No client'}")
            print(f"Date: {shift.date.date if shift.date else 'No date'}")
        else:
            print("\nNo data found in fact_shifts table.")

if __name__ == "__main__":
    check_data()
