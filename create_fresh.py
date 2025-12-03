import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app import create_app
from app.models import db, FactShift, DimEmployee, DimClient, DimJob, DimShift, DimDate

def clear_all_data():
    app = create_app()
    
    with app.app_context():
        print("CLEARING ALL DATA (Except Users)")
        print("=" * 50)
        
        try:
            # Get counts before deletion
            print("CURRENT DATA COUNTS:")
            models = {
                'FactShifts': FactShift,
                'DimEmployees': DimEmployee, 
                'DimClients': DimClient,
                'DimJobs': DimJob,
                'DimShifts': DimShift,
                'DimDates': DimDate
            }
            
            for name, model in models.items():
                count = db.session.query(model).count()
                print(f"   {name}: {count} records")
            
            # Confirm deletion
            response = input("Are you sure you want to delete all data? (yes/no): ")
            if response.lower() != 'yes':
                print("Operation cancelled.")
                return
            
            print("Deleting data...")
            
            # Delete in correct order to respect foreign keys
            db.session.query(FactShift).delete()
            print("Deleted FactShifts")
            
            db.session.query(DimShift).delete()
            print("Deleted DimShifts")
            
            db.session.query(DimDate).delete()
            print("Deleted DimDates")
            
            db.session.query(DimJob).delete()
            print("Deleted DimJobs")
            
            db.session.query(DimEmployee).delete()
            print("Deleted DimEmployees")
            
            db.session.query(DimClient).delete()
            print("Deleted DimClients")
            
            db.session.commit()
            print("All data cleared successfully!")
            
            # Verify counts after deletion
            print("FINAL DATA COUNTS:")
            for name, model in models.items():
                count = db.session.query(model).count()
                print(f"   {name}: {count} records")
                
        except Exception as e:
            db.session.rollback()
            print(f"Error clearing data: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    clear_all_data()