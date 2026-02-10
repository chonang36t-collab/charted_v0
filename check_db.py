"""Simple database check using psycopg2 directly."""
import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# Get database connection string
db_url = os.getenv('DATABASE_URL', 'postgresql://postgres:admin123@localhost:5432/sales_insight')

# Parse connection string
# Format: postgresql://user:password@host:port/database
parts = db_url.replace('postgresql://', '').split('@')
user_pass = parts[0].split(':')
host_port_db = parts[1].split('/')
host_port = host_port_db[0].split(':')

conn_params = {
    'user': user_pass[0],
    'password': user_pass[1],
    'host': host_port[0],
    'port': host_port[1] if len(host_port) > 1 else '5432',
    'database': host_port_db[1]
}

print("=" * 60)
print("DATABASE DIAGNOSTIC REPORT")
print("=" * 60)

try:
    conn = psycopg2.connect(**conn_params)
    cur = conn.cursor()
    
    # 1. Check FactShift for client data
    print("\n1. CHECKING fact_shift TABLE FOR CLIENT DATA:")
    print("-" * 60)
    
    cur.execute("SELECT COUNT(*) FROM fact_shift")
    total_shifts = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM fact_shift WHERE client_id IS NOT NULL")
    shifts_with_clients = cur.fetchone()[0]
    
    print(f"Total shifts: {total_shifts}")
    print(f"Shifts with client_id: {shifts_with_clients}")
    if total_shifts > 0:
        print(f"Percentage: {(shifts_with_clients/total_shifts*100):.1f}%")
    
    cur.execute("SELECT COUNT(DISTINCT client_id) FROM fact_shift WHERE client_id IS NOT NULL")
    unique_clients = cur.fetchone()[0]
    print(f"Unique clients: {unique_clients}")
    
    # Check dim_client table
    cur.execute("SELECT COUNT(*) FROM dim_client")
    total_dim_clients = cur.fetchone()[0]
    print(f"Total in dim_client table: {total_dim_clients}")
    
    # Get date range of data with clients
    if shifts_with_clients > 0:
        cur.execute("""
            SELECT MIN(d.date), MAX(d.date)
            FROM fact_shift f
            JOIN dim_date d ON f.date_id = d.date_id
            WHERE f.client_id IS NOT NULL
        """)
        min_date, max_date = cur.fetchone()
        print(f"Date range with clients: {min_date} to {max_date}")
    
    # 2. Check ShiftTarget table
    print("\n2. CHECKING shift_target TABLE:")
    print("-" * 60)
    
    cur.execute("SELECT COUNT(*) FROM shift_target")
    total_targets = cur.fetchone()[0]
    print(f"Total targets configured: {total_targets}")
    
    if total_targets > 0:
        cur.execute("SELECT year, month, location, site, target_count FROM shift_target LIMIT 5")
        print("\nSample targets:")
        for row in cur.fetchall():
            year, month, location, site, target = row
            print(f"  - {year}-{month}, {location}/{site or 'N/A'}: {target} shifts")
        
        cur.execute("SELECT DISTINCT year, month FROM shift_target ORDER BY year, month LIMIT 5")
        periods = cur.fetchall()
        print(f"\nTarget periods configured: {len(periods)}")
        for year, month in periods:
            print(f"  - {month} {year}")
    
    # 3. Check general shift data availability
    print("\n3. SHIFT DATA AVAILABILITY:")
    print("-" * 60)
    
    cur.execute("""
        SELECT 
            d.year,
            d.month,
            COUNT(*) as shift_count,
            COUNT(DISTINCT f.client_id) as unique_clients
        FROM fact_shift f
        JOIN dim_date d ON f.date_id = d.date_id
        GROUP BY d.year, d.month
        ORDER BY d.year DESC, 
                 CASE d.month
                    WHEN 'January' THEN 1
                    WHEN 'February' THEN 2
                    WHEN 'March' THEN 3
                    WHEN 'April' THEN 4
                    WHEN 'May' THEN 5
                    WHEN 'June' THEN 6
                    WHEN 'July' THEN 7
                    WHEN 'August' THEN 8
                    WHEN 'September' THEN 9
                    WHEN 'October' THEN 10
                    WHEN 'November' THEN 11
                    WHEN 'December' THEN 12
                 END DESC
        LIMIT 6
    """)
   
    print("Recent periods with shift data:")
    for row in cur.fetchall():
        year, month, shift_count, unique_clients = row
        print(f"  - {month} {year}: {shift_count} shifts, {unique_clients} unique clients")
    
    print("\n" + "=" * 60)
    print("RECOMMENDATIONS:")
    print("=" * 60)
    
    if shifts_with_clients == 0:
        print("⚠️  NO CLIENT DATA FOUND")
        print("   → Client associations are missing from shift records")
        print("   → Active Clients metric will show 0")
        print("   → You need to re-upload data with client information")
    
    if total_targets == 0:
        print("⚠️  NO TARGETS CONFIGURED")
        print("   → Go to Admin Metrics & Targets page")
        print("   → Set shift targets for your locations/sites")
        print("   → Required for Target Achievement metrics")
    
    if shifts_with_clients > 0 and total_targets > 0:
        print("✓ Data looks good!")
        print("  → If metrics still don't show, check date range filter")
    
    print("=" * 60)
    
    cur.close()
    conn.close()
    
except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
