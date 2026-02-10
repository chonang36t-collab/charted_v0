"""Simple SQL queries to check database - uses only standard library and database URL."""
import os
import subprocess
import json

# Get database URL from .env
db_url = "postgresql://postgres:admin36t@localhost:5432/sales_db"

# SQL queries to run
queries = [
    ("Total Shifts", "SELECT COUNT(*) FROM fact_shift;"),
    ("Shifts with Clients", "SELECT COUNT(*) FROM fact_shift WHERE client_id IS NOT NULL;"),
    ("Unique Clients", "SELECT COUNT(DISTINCT client_id) FROM fact_shift WHERE client_id IS NOT NULL;"),
    ("Total Targets", "SELECT COUNT(*) FROM shift_target;"),
    ("Recent Periods", """
        SELECT d.year, d.month, COUNT(*) as shift_count
        FROM fact_shift f
        JOIN dim_date d ON f.date_id = d.date_id
        GROUP BY d.year, d.month
        ORDER BY d.year DESC, 
                 CASE d.month
                    WHEN 'January' THEN 1 WHEN 'February' THEN 2 WHEN 'March' THEN 3
                    WHEN 'April' THEN 4 WHEN 'May' THEN 5 WHEN 'June' THEN 6
                    WHEN 'July' THEN 7 WHEN 'August' THEN 8 WHEN 'September' THEN 9
                    WHEN 'October' THEN 10 WHEN 'November' THEN 11 WHEN 'December' THEN 12
                 END DESC
        LIMIT 6;
    """),
]

print("=" * 60)
print("DATABASE DIAGNOSTIC - Using psql")
print("=" * 60)

# Parse URL to extract connection parameters
# postgresql://user:password@host:port/database
url_parts = db_url.replace('postgresql://', '').split('@')
user_pass = url_parts[0].split(':')
host_port_db = url_parts[1]

username = user_pass[0]
password = user_pass[1]
host_db = host_port_db.split('/')
host_port = host_db[0].split(':')
host = host_port[0]
port = host_port[1] if len(host_port) > 1 else '5432'
database = host_db[1]

# Set password environment variable
env = os.environ.copy()
env['PGPASSWORD'] = password

for query_name, sql in queries:
    print(f"\n{query_name}:")
    print("-" * 60)
    try:
        result = subprocess.run(
            ['psql', '-h', host, '-p', port, '-U', username, '-d', database, '-t', '-c', sql],
            capture_output=True,
            text=True,
            env=env
        )
        if result.returncode == 0:
            print(result.stdout.strip())
        else:
            print(f"ERROR: {result.stderr}")
    except FileNotFoundError:
        print("ERROR: psql command not found. Please install PostgreSQL client tools.")
        break
    except Exception as e:
        print(f"ERROR: {e}")

print("\n" + "=" * 60)
