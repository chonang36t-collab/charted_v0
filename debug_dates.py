from app import create_app, db
from app.models import DimDate

app = create_app()

with app.app_context():
    dates = db.session.query(DimDate.month, DimDate.year).distinct().limit(20).all()
    print("Distinct Month/Year samples:")
    for d in dates:
        print(f"Year: {d.year}, Month: '{d.month}' (Type: {type(d.month)})")
