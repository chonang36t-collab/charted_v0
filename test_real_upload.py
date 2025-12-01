import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))

from app import create_app, db
from app.models import DimEmployee, DimClient, DimJob, FactShift, DimShift, DimDate
from app.utils.data_loader import dbDataLoader

def test_real_upload(excel_file_path):
    """Test upload with your actual Excel file"""
    if not os.path.exists(excel_file_path):
        print(f"❌ File not found: {excel_file_path}")
        return
    
    app = create_app()
    
    with app.app_context():
        print("🚀 TESTING REAL EXCEL FILE UPLOAD")
        print("=" * 50)
        print(f"📁 File: {excel_file_path}")
        print(f"📊 Size: {os.path.getsize(excel_file_path) / 1024 / 1024:.2f} MB")
        
        print("\n📈 CURRENT DATABASE COUNTS:")
        with db.session() as session:
            print(f"   Employees: {session.query(DimEmployee).count()}")
            print(f"   Clients: {session.query(DimClient).count()}")
            print(f"   Jobs: {session.query(DimJob).count()}")
            print(f"   Shifts: {session.query(FactShift).count()}")
        
        # Load data
        loader = dbDataLoader()
        print(f"\n🔄 Loading data from {excel_file_path}...")
        
        success = loader.load_excel_data(excel_file_path)
        
        if success:
            print("\n🎉 UPLOAD SUCCESSFUL!")
            
            # Show final counts
            print("\n📊 FINAL DATABASE COUNTS:")
            with db.session() as session:
                print(f"   Employees: {session.query(DimEmployee).count()}")
                print(f"   Clients: {session.query(DimClient).count()}")
                print(f"   Jobs: {session.query(DimJob).count()}")
                print(f"   Shifts: {session.query(FactShift).count()}")
                
                # Show sample data
                print("\n👥 SAMPLE EMPLOYEES:")
                employees = session.query(DimEmployee).limit(5).all()
                for emp in employees:
                    print(f"   - {emp.full_name} (ID: {emp.employee_id})")
                    
                print("\n🏢 SAMPLE CLIENTS:")
                clients = session.query(DimClient).limit(5).all()
                for client in clients:
                    print(f"   - {client.client_name} (ID: {client.client_id})")
                
                print("\n📊 SAMPLE SHIFTS:")
                shifts = session.query(FactShift).join(DimEmployee).join(DimClient).limit(3).all()
                for shift in shifts:
                    print(f"   - {shift.employee.full_name} at {shift.client.client_name} for {shift.duration} hours")
                
        else:
            print("\n❌ UPLOAD FAILED")

if __name__ == "__main__":
    # Default file path
    excel_file_path = os.path.join(os.path.dirname(__file__), "sampleBook.xlsx")
    
    # Use command line argument if provided
    if len(sys.argv) > 1:
        excel_file_path = sys.argv[1]
    
    if not os.path.exists(excel_file_path):
        print(f"❌ Error: File not found: {excel_file_path}")
        print("Please provide a valid path to an Excel file.")
        sys.exit(1)
    
    try:
        test_real_upload(excel_file_path)
    except Exception as e:
        print(f"\n❌ An error occurred: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)