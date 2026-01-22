import pandas as pd
from datetime import datetime
import os
from sqlalchemy import text
from app.models import db, DimEmployee, DimClient, DimJob, DimShift, DimDate, FactShift

class dbDataLoader:
    def __init__(self):
        self.db_type = "PostgreSQL"
    
    def load_excel_data(self, file_path):
        """BULK load Excel data - generator for progress updates"""
        yield {"status": "progress", "message": "🚀 Starting BULK data load...", "progress": 0}
        start_time = datetime.now()
        
        try:
            # Read Excel file
            yield {"status": "progress", "message": "📄 Reading Excel file...", "progress": 5}
            df = pd.read_excel(file_path)
            yield {"status": "progress", "message": f"✓ Loaded {len(df):,} rows from Excel", "progress": 10}
            
            # Validate required columns
            required_columns = [
                'job_name', 'shift_name', 'full_name', 'location', 'site', 'role',
                'month', 'date', 'day', 'shift_start', 'shift_end', 'duration',
                'paid_hours', 'hour_rate', 'deductions', 'additions', 'total_pay',
                'client_hourly_rate', 'client_net', 'self_employed', 'dns', 'client', 'job_status'
            ]
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                yield {"status": "error", "message": f"❌ Missing columns: {missing_columns}"}
                return
            
            yield {"status": "progress", "message": "🔧 Processing data in bulk...", "progress": 15}
            
            # STEP 1: Bulk create dimension records
            yield {"status": "progress", "message": "👥 Creating dimension records...", "progress": 20}
            
            # Employees (unique)
            unique_employees = df[['full_name', 'role']].drop_duplicates()
            employees_map = self._bulk_create_employees(unique_employees)
            yield {"status": "progress", "message": f"   Employees: {len(employees_map):,} records", "progress": 25}
            
            # Clients (unique)
            unique_clients = df[['client']].drop_duplicates()
            clients_map = self._bulk_create_clients(unique_clients)
            yield {"status": "progress", "message": f"   Clients: {len(clients_map):,} records", "progress": 30}
            
            # Jobs (unique)
            unique_jobs = df[['job_name', 'location', 'site']].drop_duplicates()
            jobs_map = self._bulk_create_jobs(unique_jobs)
            yield {"status": "progress", "message": f"   Jobs: {len(jobs_map):,} records", "progress": 35}
            
            # Shifts (unique)
            unique_shifts = df[['shift_name', 'shift_start', 'shift_end']].drop_duplicates()
            shifts_map = self._bulk_create_shifts(unique_shifts)
            yield {"status": "progress", "message": f"   Shifts: {len(shifts_map):,} records", "progress": 40}
            
            # Dates (unique)
            unique_dates = df[['date', 'month', 'day']].drop_duplicates()
            dates_map = self._bulk_create_dates(unique_dates)
            yield {"status": "progress", "message": f"   Dates: {len(dates_map):,} records", "progress": 45}
            
            # STEP 2: Bulk create fact records
            yield {"status": "progress", "message": "📊 Creating fact records...", "progress": 50}
            facts_created, skipped_details = self._bulk_create_facts(df, employees_map, clients_map, jobs_map, shifts_map, dates_map)
            yield {"status": "progress", "message": f"   Fact Shifts: {facts_created:,} records", "progress": 90}
            
            # STEP 3: Verify data
            elapsed = (datetime.now() - start_time).total_seconds()
            yield {"status": "progress", "message": f"✓ BULK LOAD COMPLETE in {elapsed:.2f} seconds", "progress": 95}
            
            verification_stats = self._verify_totals(df, skipped_details)
            
            yield {
                "status": "complete", 
                "message": "Upload complete", 
                "data": {
                    "inserted": facts_created,
                    "skipped_details": skipped_details,
                    "verification": verification_stats
                }
            }
            
        except Exception as e:
            db.session.rollback()
            import traceback
            traceback.print_exc()
            yield {"status": "error", "message": f"💥 Critical error: {str(e)}"}
    
    def _bulk_create_employees(self, employees_df):
        """Bulk create employees and return mapping - FIXED to reuse existing"""
        employees_map = {}
        
        # Get existing employees - use a dictionary for O(1) lookup
        existing_employees = {}
        for emp in DimEmployee.query.all():
            # Create a composite key for lookup
            key = f"{emp.full_name}"
            existing_employees[key] = emp.employee_id
        
        new_employees = []
        for _, row in employees_df.iterrows():
            full_name = self._clean_string(row['full_name'], "Unknown Employee")
            role = self._clean_string(row.get('role', ''))
            
            # Create lookup key
            employee_key = full_name
            
            if employee_key not in employees_map:
                if employee_key in existing_employees:
                    # Reuse existing employee
                    employees_map[employee_key] = existing_employees[employee_key]
                else:
                    # Create new employee
                    new_employees.append({
                        'full_name': full_name,
                        'role': role
                    })
                    # Temporarily store as None, will update after insert
                    employees_map[employee_key] = None
        
        # Bulk insert new employees
        if new_employees:
            db.session.bulk_insert_mappings(DimEmployee, new_employees)
            db.session.commit()
            
            # Get IDs of newly inserted employees
            for emp in new_employees:
                employee = DimEmployee.query.filter_by(full_name=emp['full_name']).first()
                if employee:
                    employees_map[emp['full_name']] = employee.employee_id
        
        return employees_map

    def _bulk_create_clients(self, clients_df):
        """Bulk create clients and return mapping"""
        clients_map = {}
        
        # Get existing clients
        existing_clients = {client.client_name: client.client_id for client in DimClient.query.all()}
        
        new_clients = []
        for _, row in clients_df.iterrows():
            client_name = self._clean_string(row['client'], "Unknown Client")
            
            if client_name not in existing_clients:
                new_clients.append({
                    'client_name': client_name
                })
            clients_map[client_name] = existing_clients.get(client_name)
        
        # Bulk insert new clients
        if new_clients:
            db.session.bulk_insert_mappings(DimClient, new_clients)
            db.session.commit()
            
            # Get IDs of newly inserted clients
            for client in new_clients:
                client_obj = DimClient.query.filter_by(client_name=client['client_name']).first()
                if client_obj:
                    clients_map[client['client_name']] = client_obj.client_id
        
        return clients_map
    
    def _bulk_create_jobs(self, jobs_df):
        """Bulk create jobs and return mapping"""
        jobs_map = {}
        
        # Get existing jobs
        existing_jobs = {job.job_name: job.job_id for job in DimJob.query.all()}
        
        new_jobs = []
        for _, row in jobs_df.iterrows():
            job_name = self._clean_string(row['job_name'], "Unknown Job")
            location = self._clean_string(row.get('location', ''))
            site = self._clean_string(row.get('site', ''))
            
            job_key = f"{job_name}|{location}|{site}"
            if job_key not in jobs_map:
                if job_name not in existing_jobs:
                    new_jobs.append({
                        'job_name': job_name,
                        'location': location,
                        'site': site
                    })
                jobs_map[job_key] = existing_jobs.get(job_name)
        
        # Bulk insert new jobs
        if new_jobs:
            db.session.bulk_insert_mappings(DimJob, new_jobs)
            db.session.commit()
            
            # Get IDs of newly inserted jobs
            for job in new_jobs:
                job_obj = DimJob.query.filter_by(job_name=job['job_name']).first()
                if job_obj:
                    for key in list(jobs_map.keys()):
                        if key.startswith(job['job_name'] + '|'):
                            jobs_map[key] = job_obj.job_id
        
        return jobs_map
    
    def _bulk_create_shifts(self, shifts_df):
        """Bulk create shifts and return mapping - FIXED to reuse existing"""
        shifts_map = {}
        
        # Get existing shifts with ALL attributes
        existing_shifts = {}
        for shift in DimShift.query.all():
            # Create a composite key for exact matching
            key = f"{shift.shift_name}|{shift.shift_start}|{shift.shift_end}"
            existing_shifts[key] = shift.shift_id
        
        new_shifts = []
        for _, row in shifts_df.iterrows():
            shift_name = self._clean_string(row['shift_name'], "Unknown Shift")
            shift_start = self._clean_time(row.get('shift_start', ''))
            shift_end = self._clean_time(row.get('shift_end', ''))
            
            shift_key = f"{shift_name}|{shift_start}|{shift_end}"
            
            if shift_key not in shifts_map:
                if shift_key in existing_shifts:
                    # Reuse existing shift
                    shifts_map[shift_key] = existing_shifts[shift_key]
                else:
                    # Create new shift
                    new_shifts.append({
                        'shift_name': shift_name,
                        'shift_start': shift_start,
                        'shift_end': shift_end
                    })
                    # Temporarily store as None
                    shifts_map[shift_key] = None
        
        # Bulk insert new shifts
        if new_shifts:
            db.session.bulk_insert_mappings(DimShift, new_shifts)
            db.session.commit()
            
            # Get IDs of newly inserted shifts
            for shift in new_shifts:
                shift_key = f"{shift['shift_name']}|{shift['shift_start']}|{shift['shift_end']}"
                shift_obj = DimShift.query.filter_by(
                    shift_name=shift['shift_name'],
                    shift_start=shift['shift_start'],
                    shift_end=shift['shift_end']
                ).first()
                if shift_obj:
                    shifts_map[shift_key] = shift_obj.shift_id
        
        return shifts_map
    
    def _bulk_create_dates(self, dates_df):
        """Bulk create dates and return mapping - FIXED to reuse existing"""
        dates_map = {}
        
        # Get existing dates
        existing_dates = {}
        for date_obj in DimDate.query.all():
            # date is stored as string 'YYYY-MM-DD'
            existing_dates[str(date_obj.date)] = date_obj.date_id
        
        
        new_dates = []
        for _, row in dates_df.iterrows():
            date_value = row['date']
            month = self._clean_string(row.get('month', ''))
            day = self._clean_string(row.get('day', ''))
            
            # Clean date
            date_str, date_id = self._clean_date_with_id(date_value)
            
            if date_str not in dates_map:
                if date_str in existing_dates:
                    # Reuse existing date
                    dates_map[date_str] = existing_dates[date_str]
                else:
                    # Create new date
                    new_dates.append({
                        'date_id': date_id,
                        'date': date_str,
                        'day': day,
                        'month': month,
                        'year': date_id // 10000
                    })
                    # Temporarily store calculated ID
                    dates_map[date_str] = date_id
        
        # Bulk insert new dates
        if new_dates:
            try:
                db.session.bulk_insert_mappings(DimDate, new_dates)
                db.session.commit()
                
                # Update with actual IDs from DB (should match date_id)
                for date_record in new_dates:
                    date_obj = DimDate.query.filter_by(date=date_record['date']).first()
                    if date_obj:
                        dates_map[date_record['date']] = date_obj.date_id
            except Exception as e:
                db.session.rollback()
                # If duplicate key error, dates might already exist
                # Try to fetch them
                for date_record in new_dates:
                    date_obj = DimDate.query.filter_by(date=date_record['date']).first()
                    if date_obj:
                        dates_map[date_record['date']] = date_obj.date_id
        
        return dates_map
  
    def _bulk_create_facts(self, df, employees_map, clients_map, jobs_map, shifts_map, dates_map):
        fact_records = []
        skipped_details = []
        
        
        # Get ALL existing fact records for proper deduplication
        existing_keys = set()
        try:
            query = text("SELECT employee_id, date_id, shift_id FROM fact_shifts")
            result = db.session.execute(query)
            for row in result:
                key = (int(row[0]), int(row[1]), int(row[2]))
                existing_keys.add(key)
        except Exception as e:
            print(f"Error fetching existing fact records: {e}")
        
        # Track duplicates within current file
        seen_in_current_file = {}
        
        # Process each row
        for idx, row in df.iterrows():
            # Get foreign keys
            full_name = self._clean_string(row['full_name'], "Unknown Employee")
            client_name = self._clean_string(row['client'], "Unknown Client")
            
            job_name = self._clean_string(row['job_name'], "Unknown Job")
            location = self._clean_string(row.get('location', ''))
            site = self._clean_string(row.get('site', ''))
            job_key = f"{job_name}|{location}|{site}"
            
            shift_name = self._clean_string(row['shift_name'], "Unknown Shift")
            shift_start = self._clean_time(row.get('shift_start', ''))
            shift_end = self._clean_time(row.get('shift_end', ''))
            shift_key = f"{shift_name}|{shift_start}|{shift_end}"
            
            # Date processing
            date_value = row['date']
            date_str, _ = self._clean_date_with_id(date_value)
            
            # Get IDs
            employee_id = employees_map.get(full_name)
            client_id = clients_map.get(client_name)
            job_id = jobs_map.get(job_key)
            shift_id = shifts_map.get(shift_key)
            date_id = dates_map.get(date_str)
            
            # Skip if missing keys
            if None in (employee_id, date_id, shift_id):
                missing = []
                if not employee_id: missing.append(f"Employee: {full_name}")
                if not date_id: missing.append(f"Date: {date_str}")
                if not shift_id: missing.append(f"Shift: {shift_key}")
                
                skipped_details.append({
                    "row": idx + 2,
                    "reason": "Missing Keys",
                    "details": ", ".join(missing)
                })
                continue
            
            # Create unique key
            key = (int(employee_id), int(date_id), int(shift_id))
            
            # Check for duplicates in database
            if key in existing_keys:
                skipped_details.append({
                    "row": idx + 2,
                    "reason": "Duplicate",
                    "details": f"{full_name} on {date_str} (Shift: {shift_name})",
                    "duplicate_type": "pre_existing"
                })
                continue
            
            # Check for duplicates within the current file
            if key in seen_in_current_file:
                original_row = seen_in_current_file[key]
                skipped_details.append({
                    "row": idx + 2,
                    "reason": "Duplicate",
                    "details": f"{full_name} on {date_str} (Shift: {shift_name}) - Duplicate of row {original_row}",
                    "duplicate_type": "intra_file"
                })
                continue
            
            # Handle formula in client_net
            client_net_value = row.get('client_net', 0)
            if isinstance(client_net_value, str) and client_net_value.startswith('='):
                try:
                    hourly_rate = self._safe_float(row.get('client_hourly_rate', 0))
                    paid_hours = self._safe_float(row.get('paid_hours', 0))
                    client_net_value = hourly_rate * paid_hours
                except:
                    client_net_value = 0
            
            # Create fact record
            fact_record = {
                'employee_id': employee_id,
                'client_id': client_id,
                'job_id': job_id,
                'shift_id': shift_id,
                'date_id': date_id,
                'duration': self._safe_float(row.get('duration', 0)),
                'paid_hours': self._safe_float(row.get('paid_hours', 0)),
                'hour_rate': self._safe_float(row.get('hour_rate', 0)),
                'deductions': self._safe_float(row.get('deductions', 0)),
                'additions': self._safe_float(row.get('additions', 0)),
                'total_pay': self._safe_float(row.get('total_pay', 0)),
                'client_hourly_rate': self._safe_float(row.get('client_hourly_rate', 0)),
                'client_net': self._safe_float(client_net_value),
                'self_employed': self._safe_bool(row.get('self_employed', False)),
                'dns': self._safe_bool(row.get('dns', False)),
                'job_status': self._clean_string(row.get('job_status', ''))[:50]  # Truncate to match DB constraints
            }
            
            fact_records.append(fact_record)
            seen_in_current_file[key] = idx + 2
            existing_keys.add(key)
        
        print(f"\n=== SUMMARY ===")
        print(f"Total rows: {len(df)}")
        print(f"To insert: {len(fact_records)}")
        print(f"Skipped: {len(skipped_details)}")
        
        # Bulk insert
        total_inserted = 0
        if fact_records:
            batch_size = 1000
            for i in range(0, len(fact_records), batch_size):
                batch = fact_records[i:i + batch_size]
                db.session.bulk_insert_mappings(FactShift, batch)
                db.session.commit()
                total_inserted += len(batch)
                print(f"Inserted batch {i//batch_size + 1}: {len(batch):,} records")
        
        return total_inserted, skipped_details
    
    def _clean_string(self, value, default=""):
        """Clean string values, handle NaN and None"""
        if pd.isna(value) or value is None:
            return default
        cleaned = str(value).strip()
        return cleaned if cleaned and cleaned.lower() != 'nan' else default
    
    def _clean_time(self, value):
        """Clean time values, handle invalid formats"""
        if pd.isna(value) or value is None:
            return "00:00:00"
        
        try:
            # If it's already a time object
            if hasattr(value, 'strftime'):
                return value.strftime('%H:%M:%S')
            
            # If it's a string
            value_str = str(value).strip().lower()
            if not value_str or value_str == 'nan':
                return "00:00:00"
            
            # Try to parse various time formats
            if ':' in value_str:
                # Already in time format
                parts = value_str.split(':')
                if len(parts) >= 2:
                    hours = parts[0].zfill(2)
                    minutes = parts[1].zfill(2)
                    seconds = parts[2].zfill(2) if len(parts) > 2 else '00'
                    return f"{hours}:{minutes}:{seconds}"
            
            # If it's a number, treat as hours
            try:
                hours = float(value_str)
                if 0 <= hours < 24:
                    return f"{int(hours):02d}:00:00"
            except:
                pass
                
            return "00:00:00"
        except:
            return "00:00:00"
    
    def _clean_date_with_id(self, date_value):
        """Clean date values and return both string and ID - IMPROVED"""
        default_date = "2000-01-01"
        default_id = 20000101
        
        if pd.isna(date_value) or date_value is None:
            return default_date, default_id
        
        try:
            # Handle Excel formulas or strings
            if isinstance(date_value, str):
                # Remove formula if present
                if date_value.startswith('='):
                    # Extract just the date part if it's a formula
                    date_str = str(date_value)
                    # Try to find a date pattern in the string
                    import re
                    date_match = re.search(r'\d{4}-\d{2}-\d{2}', date_str)
                    if date_match:
                        date_str = date_match.group(0)
                    else:
                        return default_date, default_id
                else:
                    date_str = date_value
            else:
                date_str = str(date_value)
            
            # Clean the date string
            date_str = date_str.strip()
            
            # If it's already a datetime-like string with time
            if ' ' in date_str:
                date_str = date_str.split()[0]
            
            # Remove time part if present
            if ' 00:00:00' in date_str:
                date_str = date_str.replace(' 00:00:00', '')
            
            # Try parsing with pandas (handles Excel dates well)
            try:
                parsed_date = pd.to_datetime(date_str, errors='raise')
                date_id = parsed_date.year * 10000 + parsed_date.month * 100 + parsed_date.day
                return parsed_date.strftime('%Y-%m-%d'), date_id
            except:
                # Try manual parsing
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y.%m.%d', '%d-%m-%Y', '%m-%d-%Y']:
                    try:
                        parsed_date = datetime.strptime(date_str, fmt)
                        date_id = parsed_date.year * 10000 + parsed_date.month * 100 + parsed_date.day
                        return parsed_date.strftime('%Y-%m-%d'), date_id
                    except:
                        continue
                
                return default_date, default_id
        except Exception as e:
            print(f"Date parsing error for '{date_value}': {e}")
            return default_date, default_id
    
    def _derive_month_day(self, date_str):
        """Derive month and day names from date string"""
        try:
            parsed_date = datetime.strptime(date_str, '%Y-%m-%d')
            month = parsed_date.strftime('%B')  # Full month name
            day = parsed_date.strftime('%A')    # Full day name
            return month, day
        except:
            return "Unknown", "Unknown"
    
    def _safe_float(self, value):
        if pd.isna(value) or value is None:
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    def _safe_bool(self, value):
        if pd.isna(value) or value is None:
            return False
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        if isinstance(value, str):
            return value.lower() in ['true', 'yes', '1', 'y']
        return False
    
    def _verify_totals(self, df, skipped_details=None):
        """Verify that loaded data matches Excel totals"""
        try:
            from sqlalchemy import text
            
            # Ensure numeric columns are actually numeric
            df['client_net'] = pd.to_numeric(df['client_net'], errors='coerce').fillna(0)
            df['total_pay'] = pd.to_numeric(df['total_pay'], errors='coerce').fillna(0)
            df['paid_hours'] = pd.to_numeric(df['paid_hours'], errors='coerce').fillna(0)
            
            # Excel totals (Raw)
            raw_excel_revenue = df['client_net'].sum()
            excel_cost = df['total_pay'].sum()
            excel_hours = df['paid_hours'].sum()
            
            # Calculate Skipped Revenue
            skipped_revenue = 0
            if skipped_details:
                for item in skipped_details:
                    should_subtract = False
                    # Subtract if it was skipped due to missing keys
                    if item.get('reason') == 'Missing Keys':
                        should_subtract = True
                    # Subtract if it was a duplicate WITHIN the file (intra-file)
                    elif item.get('reason') == 'Duplicate' and item.get('duplicate_type') == 'intra_file':
                        should_subtract = True
                    
                    if should_subtract:
                        row_idx = item['row'] - 2 # Convert back to 0-based index
                        if 0 <= row_idx < len(df):
                            val = df.iloc[row_idx]['client_net']
                            skipped_revenue += val
            
            adjusted_excel_revenue = raw_excel_revenue - skipped_revenue
            
            # Determine date range from dataframe to scope the DB query
            date_ids = []
            for date_val in df['date'].unique():
                _, date_id = self._clean_date_with_id(date_val)
                date_ids.append(date_id)
            
            if not date_ids:
                return {"excel_revenue": 0, "db_revenue": 0, "match": False, "error": "No dates in file"}
                
            min_date = min(date_ids)
            max_date = max(date_ids)
            
            # Database totals - SCOPED to the file's date range
            sql = text("""
                SELECT 
                    COALESCE(SUM(client_net), 0) as revenue,
                    COALESCE(SUM(total_pay), 0) as cost,
                    COALESCE(SUM(paid_hours), 0) as hours
                FROM fact_shifts
                WHERE date_id BETWEEN :min_date AND :max_date
            """)
            result = db.session.execute(sql, {"min_date": min_date, "max_date": max_date}).fetchone()
            db_revenue = float(result[0])
            db_cost = float(result[1])
            db_hours = float(result[2])
            
            print(f"DATA VERIFICATION (Range: {min_date} - {max_date}):")
            print(f"   Raw Excel Revenue:      ${raw_excel_revenue:,.2f}")
            print(f"   Skipped Revenue:        ${skipped_revenue:,.2f}")
            print(f"   Adjusted Excel Revenue: ${adjusted_excel_revenue:,.2f}")
            print(f"   DB Revenue:             ${db_revenue:,.2f}")
            print(f"   Match: {abs(db_revenue - adjusted_excel_revenue) < 1.0}")
            
            match = bool(abs(db_revenue - adjusted_excel_revenue) < 1.0)
            if match:
                print("TOTALS MATCH!")
            else:
                print("TOTALS DON'T MATCH!")
            
            return {
                "excel_revenue": float(adjusted_excel_revenue), # Return adjusted for UI comparison
                "db_revenue": float(db_revenue),
                "match": match
            }
                
        except Exception as e:
            print(f"Verification error: {e}")
            return {
                "excel_revenue": 0,
                "db_revenue": 0,
                "match": False,
                "error": str(e)
            }
    
    def cleanup_duplicates(self):
        """Remove duplicate facts for testing"""
        try:
            # Find duplicates
            sql = text("""
                DELETE FROM fact_shifts 
                WHERE id IN (
                    SELECT id FROM (
                        SELECT id, 
                               ROW_NUMBER() OVER (
                                   PARTITION BY employee_id, date_id, shift_id 
                                   ORDER BY id
                               ) as rn
                        FROM fact_shifts
                    ) t
                    WHERE t.rn > 1
                )
            """)
            result = db.session.execute(sql)
            db.session.commit()
            print(f"✓ Removed {result.rowcount} duplicate rows from fact_shifts")
        except Exception as e:
            print(f"Error cleaning duplicates: {e}")
            db.session.rollback()
    
    def check_database_constraints(self):
        """Check if the UNIQUE constraint exists and create it if not"""
        try:
            # Check if constraint already exists
            sql = text("""
                SELECT constraint_name 
                FROM information_schema.table_constraints 
                WHERE table_name = 'fact_shifts' 
                AND constraint_type = 'UNIQUE'
                AND constraint_name = 'unique_employee_date_shift'
            """)
            result = db.session.execute(sql).fetchone()
            
            if not result:
                # Create the constraint
                sql = text("""
                    ALTER TABLE fact_shifts 
                    ADD CONSTRAINT unique_employee_date_shift 
                    UNIQUE (employee_id, date_id, shift_id)
                """)
                db.session.execute(sql)
                db.session.commit()
                print("✓ Created UNIQUE constraint on fact_shifts (employee_id, date_id, shift_id)")
                return True
            else:
                print("✓ UNIQUE constraint already exists")
                return True
        except Exception as e:
            print(f"Error creating constraint: {e}")
            db.session.rollback()
            return False