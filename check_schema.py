from app import create_app
from app.models import db, FactShift, DimClient, DimDate

def check_schema():
    app = create_app()
    with app.app_context():
        # Check table columns
        print("\n=== FactShift Columns ===")
        for column in FactShift.__table__.columns:
            print(f"{column.name}: {column.type}")
            
        # Check sample data with joins
        print("\n=== Sample Data with Joins ===")
        sample = db.session.query(
            FactShift, DimClient, DimDate
        ).join(
            DimClient, FactShift.client_id == DimClient.client_id
        ).join(
            DimDate, FactShift.date_id == DimDate.date_id
        ).first()
        
        if sample:
            fact_shift, client, date = sample
            print(f"FactShift: {fact_shift.shift_record_id}, Client: {client.client_name}, Date: {date.date}")
            print(f"Client Net: {fact_shift.client_net}, Total Pay: {fact_shift.total_pay}")
        else:
            print("No data found with joins. Checking why...")
            
            # Check if there are any records in the tables
            print("\n=== Record Counts ===")
            print(f"FactShift: {FactShift.query.count()}")
            print(f"DimClient: {DimClient.query.count()}")
            print(f"DimDate: {DimDate.query.count()}")
            
            # Check if there are any client_id or date_id in fact_shifts that don't exist in dim tables
            if FactShift.query.count() > 0:
                print("\n=== Checking for Orphaned Records ===")
                # Check for client_ids in fact_shifts that don't exist in dim_clients
                bad_clients = db.session.query(FactShift.client_id).filter(
                    ~FactShift.client_id.in_(db.session.query(DimClient.client_id))
                ).distinct().all()
                
                if bad_clients:
                    print(f"Found {len(bad_clients)} fact_shifts with invalid client_id")
                    print("Example client_ids:", [c[0] for c in bad_clients[:5]])
                
                # Check for date_ids in fact_shifts that don't exist in dim_dates
                bad_dates = db.session.query(FactShift.date_id).filter(
                    ~FactShift.date_id.in_(db.session.query(DimDate.date_id))
                ).distinct().all()
                
                if bad_dates:
                    print(f"Found {len(bad_dates)} fact_shifts with invalid date_id")
                    print("Example date_ids:", [d[0] for d in bad_dates[:5]])

if __name__ == "__main__":
    check_schema()
