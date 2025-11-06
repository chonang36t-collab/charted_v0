# Sales Insight — Flask + SQLite LAN dashboard

A local, LAN-accessible Flask app for uploading Excel sales data, validating and storing it in SQLite, and exploring insights via an interactive Plotly dashboard. Authentication is provided with roles (admin/viewer).

## Features
- Authentication (Flask-Login) with admin and viewer roles.
- Admin upload of .xlsx files with validation and deduplication.
- SQLAlchemy models backed by SQLite at `instance/sales_data.db`.
- Analysis utilities exposed as a JSON API consumed by Plotly.js.
- Bootstrap 5 responsive UI.

## Project structure
```
sales_dashboard/
├─ app/
│  ├─ __init__.py
│  ├─ routes.py
│  ├─ auth.py
│  ├─ models.py
│  ├─ utils/
│  │   ├─ data_loader.py
│  │   └─ analysis.py
│  ├─ templates/
│  │   ├─ base.html
│  │   ├─ login.html
│  │   ├─ dashboard.html
│  │   ├─ upload.html
│  │   └─ users.html
│  └─ static/
├─ instance/
│  └─ sales_data.db
├─ run.py
├─ requirements.txt
├─ .env.example
└─ README.md
```

## Setup (Windows)

- Create a virtual environment (Python 3.11+):
```
py -3.11 -m venv .venv
.venv\\Scripts\\activate
```

- Install dependencies:
```
pip install -r requirements.txt
```

- Create `.env` from example:
```
copy .env.example .env
```
Edit `.env` and set:
- `SECRET_KEY` to a long random string
- Optionally `DATABASE_URL`. If omitted, app uses `sqlite:///instance/sales_data.db` automatically.

- Initialize the database and create an admin user:
```
python run.py init-db
python run.py create-admin --username admin --password admin123 --role admin
```

- Run the app on LAN:
```
python run.py run --host 0.0.0.0 --port 5000
```
Access from other machines on the LAN using `http://<your_machine_ip>:5000`.

## Upload format
Upload a `.xlsx` file with a header row containing exactly these columns:
```
date, order_id, product_id, product_name, category, region, quantity, unit_price, unit_cost
```
Derived fields are computed automatically: `sales`, `cost`, `profit`, `profit_margin`.

## Minimal test
You can create a simple Excel with the header above and a few rows like:
```
2025-01-01,ORD-1,PROD-1,Widget A,Widgets,East,10,20,12
2025-01-02,ORD-2,PROD-2,Widget B,Widgets,West,5,25,10
```
Upload via the Upload page (admin only) and open the Dashboard.

## Notes
- Frontend libraries are loaded via CDN for convenience. For offline or no-internet environments, download assets and update `base.html` to point to local files.
- The SQLite database file is created in `instance/` on first run if not present.
# charted
# charted
