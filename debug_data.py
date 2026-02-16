from app import create_app, db
from app.models import FactShift, DimDate
from sqlalchemy import func

app = create_app()

with app.app_context():
    print("Checking Financial Data...")
    
    # Check DimDate sample
    date_sample = DimDate.query.first()
    if date_sample:
        print(f"DimDate Sample: ID={date_sample.date_id}, Date={date_sample.date}, Month='{date_sample.month}', Year={date_sample.year}")
    else:
        print("DimDate is empty!")

    # Check distinct months in DimDate
    months = db.session.query(DimDate.month).distinct().all()
    print("Distinct Months in DB:")
    for m in months:
        print(f"'{m.month}'")

