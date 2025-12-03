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
        yield {"status": "progress", "message": "ðŸš€ Starting BULK data load...", "progress": 0}
        start_time = datetime.now()
        
        try:
            # Read Excel file
            yield {"status": "progress", "message": "ðŸ“– Reading Excel file...", "progress": 5}
            df = pd.read_excel(file_path)
            yield {"status": "progress", "message": f"âœ… Loaded {len(df):,} rows from Excel", "progress": 10}
            
            # Validate required columns
            required_columns = [
                'job_name', 'shift_name', 'full_name', 'location', 'site', 'role',
                'month', 'date', 'day', 'shift_start', 'shift_end', 'duration',
                'paid_hours', 'hour_rate', 'deductions', 'additions', 'total_pay',
                'client_hourly_rate', 'client_net', 'self_employed', 'dns', 'client', 'job_status'
            ]
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                yield {"status": "error", "message": f"âŒ Missing columns: {missing_columns}"}
                return
            
            yield {"status": "progress", "message": "ðŸ”„ Processing data in bulk...", "progress": 15}
            
            # STEP 1: Bulk create dimension records
            yield {"status": "progress", "message": "ðŸ‘¥ Creating dimension records...", "progress": 20}
            
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
            yield {"status": "progress", "message": "ðŸ“Š Creating fact records...", "progress": 50}
            facts_created, skipped_details = self._bulk_create_facts(df, employees_map, clients_map, jobs_map, shifts_map, dates_map)
            yield {"status": "progress", "message": f"   Fact Shifts: {facts_created:,} records", "progress": 90}
            
            # STEP 3: Verify data
            elapsed = (datetime.now() - start_time).total_seconds()
            yield {"status": "progress", "message": f"âœ… BULK LOAD COMPLETE in {elapsed:.2f} seconds", "progress": 95}
            
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
            yield {"status": "error", "message": f"ðŸ’¥ Critical error: {str(e)}"}
    
    def _bulk_create_employees(self, employees_df):
        """Bulk create employees and return mapping"""
        employees_map = {}
        
        # Get existing employees
        existing_employees = {emp.full_name: emp.employee_id for emp in DimEmployee.query.all()}
        
        new_employees = []
        for _, row in employees_df.iterrows():
            full_name = self._clean_string(row['full_name'], "Unknown Employee")
            role = self._clean_string(row.get('role', ''))
            
            if full_name not in existing_employees:
                new_employees.append({
                    'full_name': full_name,
                    'role': role
                })
            employees_map[full_name] = existing_employees.get(full_name)
        
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
                    for key in jobs_map:
                        if key.startswith(job['job_name'] + '|'):
                            jobs_map[key] = job_obj.job_id
        
        return jobs_map
    
    def _bulk_create_shifts(self, shifts_df):
        """Bulk create shifts and return mapping - FIXED for invalid time values"""
        shifts_map = {}
        
        # Get existing shifts
        existing_shifts = {shift.shift_name: shift.shift_id for shift in DimShift.query.all()}
        
        new_shifts = []
        # NEW: Map identifier to original key to avoid reconstruction issues
        new_shift_keys = {} 
        
        for _, row in shifts_df.iterrows():
            shift_name = self._clean_string(row['shift_name'], "Unknown Shift")
            shift_start = self._clean_time(row.get('shift_start', ''))
            shift_end = self._clean_time(row.get('shift_end', ''))
            
            shift_key = f"{shift_name}|{shift_start}|{shift_end}"
            shift_identifier = f"{shift_name}_{shift_start}_{shift_end}"
            
            if shift_key not in shifts_map:
                if shift_identifier not in existing_shifts:
                    new_shifts.append({
                        'shift_name': shift_identifier,
                        'shift_start': shift_start,
                        'shift_end': shift_end
                    })
                    # NEW: Store the mapping
                    new_shift_keys[shift_identifier] = shift_key
                    
                shifts_map[shift_key] = existing_shifts.get(shift_identifier)
        
        # Debug logging
        found_count = len([v for v in shifts_map.values() if v is not None])
        print(f"   Shift Mapping: {len(shifts_map)} unique shifts, {found_count} found in DB, {len(new_shifts)} new")
        
        # Bulk insert new shifts in smaller batches to identify problematic records
        if new_shifts:
            # Insert in smaller batches to catch errors
            batch_size = 100
            for i in range(0, len(new_shifts), batch_size):
                batch = new_shifts[i:i + batch_size]
                try:
                    db.session.bulk_insert_mappings(DimShift, batch)
                    db.session.commit()
                    print(f"Inserted shift batch {i//batch_size + 1}")
                except Exception as e:
                    print(f"Error in shift batch {i//batch_size + 1}: {e}")
                    # Try individual inserts to identify the problematic record
                    for j, shift in enumerate(batch):
                        try:
                            db.session.bulk_insert_mappings(DimShift, [shift])
                            db.session.commit()
                        except Exception as single_error:
                            print(f"Problematic shift: {shift}")
                            # Skip this problematic shift
                            continue
            
            # Get IDs of newly inserted shifts
            for shift in new_shifts:
                shift_obj = DimShift.query.filter_by(shift_name=shift['shift_name']).first()
                if shift_obj:
                    # NEW: Use the stored key directly
                    original_key = new_shift_keys.get(shift['shift_name'])
                    if original_key:
                        shifts_map[original_key] = shift_obj.shift_id
        
        return shifts_map
    
    def _bulk_create_dates(self, dates_df):
        """Bulk create dates and return mapping - FIXED for invalid date values"""
        dates_map = {}
        
        # Get existing dates
        existing_dates = {date.date: date.date_id for date in DimDate.query.all()}
        
        new_dates = []
        problematic_dates = []
        
        for _, row in dates_df.iterrows():
            date_value = row['date']
            month = self._clean_string(row.get('month', ''))
            day = self._clean_string(row.get('day', ''))
            
            # Convert date to string with robust cleaning
            date_str, date_id = self._clean_date_with_id(date_value)
            
            # Derive month and day from date if not provided
            if not month or not day:
                derived_month, derived_day = self._derive_month_day(date_str)
                month = month or derived_month
                day = day or derived_day
            
            if date_str not in dates_map:
                if date_str not in existing_dates:
                    # Validate date_id is reasonable
                    if date_id < 19000101 or date_id > 21000101:
                        problematic_dates.append({
                            'original_date': date_value,
                            'cleaned_date': date_str,
                            'date_id': date_id
                        })
                        # Use a safe default
                        date_id = 20000101
                        date_str = "2000-01-01"
                    
                    new_dates.append({
                        'date_id': date_id,
                        'date': date_str,
                        'day': day,
                        'month': month,
                        'year': date_id // 10000
                    })
                dates_map[date_str] = existing_dates.get(date_str, date_id)
        
        # Log problematic dates
        if problematic_dates:
            print(f"Found {len(problematic_dates)} problematic dates, using defaults")
            for prob in problematic_dates[:5]:  # Show first 5
                print(f"Problematic: {prob['original_date']} -> {prob['cleaned_date']} (ID: {prob['date_id']})")
        
        # Bulk insert new dates
        if new_dates:
            db.session.bulk_insert_mappings(DimDate, new_dates)
            db.session.commit()
        
        return dates_map
    
    def _bulk_create_facts(self, df, employees_map, clients_map, jobs_map, shifts_map, dates_map):
        """Bulk create fact records with deduplication"""
        fact_records = []
        skipped_details = []
        
        # Pre-fetch existing records to check for duplicates
        # We'll use a composite key of (employee_id, date_id, shift_id) to identify unique assignments
        existing_keys = set()
        try:
            # Get the date range for the current upload to limit the query
            # We need to convert the dates in df to date_ids to query efficiently
            upload_date_ids = set()
            for date_val in df['date'].unique():
                _, date_id = self._clean_date_with_id(date_val)
                upload_date_ids.add(date_id)
            
            if upload_date_ids:
                min_date_id = min(upload_date_ids)
                max_date_id = max(upload_date_ids)
                
                query = text("""
                    SELECT employee_id, date_id, shift_id 
                    FROM fact_shifts 
                    WHERE date_id BETWEEN :min_date AND :max_date
                """)
                result = db.session.execute(query, {"min_date": min_date_id, "max_date": max_date_id})
                for row in result:
                    existing_keys.add((row[0], row[1], row[2]))
                print(f"   Dedup Check: Fetched {len(existing_keys)} existing fact records from DB")
        except Exception as e:
            print(f"Warning: Could not pre-fetch existing records for deduplication: {e}")
            
        # Keep a copy of initial DB keys to distinguish between 
        # "pre-existing in DB" vs "duplicate within this file"
        initial_db_keys = existing_keys.copy()
        
        # Track ALL rows with the same key (for showing all duplicate row numbers)
        # Map: (employee_id, date_id, shift_id) -> list of row indices
        seen_keys_in_file = {}

        for idx, row in df.iterrows():
            # Get foreign keys from mappings
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
            
            # Get foreign keys
            employee_id = employees_map.get(full_name)
            client_id = clients_map.get(client_name)
            job_id = jobs_map.get(job_key)
            shift_id = shifts_map.get(shift_key)
            date_id = dates_map.get(date_str)
            
            # Skip if any required foreign key is missing
            if not all([employee_id, client_id, job_id, shift_id, date_id]):
                missing = []
                if not employee_id: missing.append(f"Employee: {full_name}")
                if not client_id: missing.append(f"Client: {client_name}")
                if not job_id: missing.append(f"Job: {job_name}")
                if not shift_id: missing.append(f"Shift: {shift_name}")
                if not date_id: missing.append(f"Date: {date_str}")
                
                skipped_details.append({
                    "row": idx + 2, # Excel is 1-indexed + header
                    "reason": "Missing Keys",
                    "details": ", ".join(missing)
                })
                if len(skipped_details) <= 10:
                    print(f"Skipping row {idx}: Missing {', '.join(missing)}")
                continue
            
            # Check for duplicate
            key = (employee_id, date_id, shift_id)
            
            # DEBUG: Log first failure
            in_existing = key in existing_keys
            if not in_existing and idx > 10 and 'first_failure_logged' not in locals():
                first_failure_logged = True
                print(f"\n🔴 FIRST FAILURE at Row {idx + 2}:")
                print(f"   Key: {key}")
                print(f"   Types: ({type(employee_id)}, {type(date_id)}, {type(shift_id)})")
                
                # Find close matches
                print("   Searching for close matches in existing_keys...")
                found_close = False
                for ek in list(existing_keys)[:10000]: # Check first 10k to be safe
                    # Check if 2 out of 3 match
                    matches = sum([1 for i in range(3) if ek[i] == key[i]])
                    if matches >= 2:
                        print(f"   Found CLOSE MATCH: {ek}")
                        print(f"      Diff: Emp={ek[0]==key[0]}, Date={ek[1]==key[1]}, Shift={ek[2]==key[2]}")
                        found_close = True
                        break
                if not found_close:
                    print("   No close matches found in sample.")
            
            if key in existing_keys:
                is_db_dup = key in initial_db_keys
                dup_type = "DB (Pre-existing)" if is_db_dup else "File (Intra-file)"
                
                details_msg = f"{full_name} on {date_str} ({shift_name}) [DateID: {date_id}]"
                
                # Show ALL duplicate row numbers
                if not is_db_dup and key in seen_keys_in_file:
                    dup_rows = [r + 2 for r in seen_keys_in_file[key]]  # Convert to Excel rows
                    rows_str = ", ".join(map(str, dup_rows))
                    details_msg += f" - Duplicate of Row(s): {rows_str}"
                    dup_type += f" (Rows: {rows_str})"
                
                skipped_details.append({
                    "row": idx + 2,
                    "reason": "Duplicate",
                    "details": details_msg,
                    "duplicate_type": "pre_existing" if is_db_dup else "intra_file"
                })
                
                if len([s for s in skipped_details if s['reason'] == 'Duplicate']) <= 10:
                    print(f"   ℹ️  Skipping duplicate row {idx + 2}: {full_name} on {date_str} - {dup_type}")
                continue
            
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
                'client_net': self._safe_float(row.get('client_net', 0)),
                'self_employed': self._safe_bool(row.get('self_employed', False)),
                'dns': self._safe_bool(row.get('dns', False)),
                'job_status': self._clean_string(row.get('job_status', ''))
            }
            
            fact_records.append(fact_record)
            # Add to existing keys to prevent duplicates within the same file
            existing_keys.add(key)
            # Track this row for duplicate reporting
            if key not in seen_keys_in_file:
                seen_keys_in_file[key] = []
            seen_keys_in_file[key].append(idx)
        
        # Summary of what happened
        total_rows = len(df)
        skipped_count = len(skipped_details)
        inserted_count = len(fact_records)
        print(f"\n   SUMMARY: Total rows={total_rows}, Skipped={skipped_count}, To insert={inserted_count}")
        print(f"   Existing keys in DB: {len(initial_db_keys)}, Final existing keys: {len(existing_keys)}\n")
        
        
        # Bulk insert all fact records in batches
        total_inserted = 0
        if fact_records:
            batch_size = 1000
            
            for i in range(0, len(fact_records), batch_size):
                batch = fact_records[i:i + batch_size]
                db.session.bulk_insert_mappings(FactShift, batch)
                db.session.commit()
                total_inserted += len(batch)
                print(f"Inserted fact batch {i//batch_size + 1}: {len(batch):,} records")
        
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
        """Clean date values and return both string and ID - FIXED for invalid dates"""
        default_date = "2000-01-01"
        default_id = 20000101
        
        if pd.isna(date_value) or date_value is None:
            return default_date, default_id
        
        try:
            # If it's already a datetime object
            if isinstance(date_value, datetime):
                date_str = date_value.strftime('%Y-%m-%d')
                date_id = date_value.year * 10000 + date_value.month * 100 + date_value.day
                return date_str, date_id
            
            # If it's a string
            date_str = str(date_value).strip()
            if not date_str or date_str.lower() == 'nan':
                return default_date, default_id
            
            # Clean the date string
            date_str = date_str.split()[0]  # Take only date part
            date_str = date_str.replace(' 00:00:00', '').strip()
            
            # Try to parse the date
            try:
                # Try common date formats
                for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y.%m.%d', '%d-%m-%Y', '%m-%d-%Y']:
                    try:
                        parsed_date = datetime.strptime(date_str, fmt)
                        date_id = parsed_date.year * 10000 + parsed_date.month * 100 + parsed_date.day
                        return parsed_date.strftime('%Y-%m-%d'), date_id
                    except:
                        continue
                
                # If no format works, try to extract numbers
                numbers = ''.join(filter(str.isdigit, date_str))
                if len(numbers) >= 8:
                    year = int(numbers[:4])
                    month = int(numbers[4:6])
                    day = int(numbers[6:8])
                    if 1900 <= year <= 2100 and 1 <= month <= 12 and 1 <= day <= 31:
                        parsed_date = datetime(year, month, day)
                        date_id = year * 10000 + month * 100 + day
                        return parsed_date.strftime('%Y-%m-%d'), date_id
            except:
                pass
            
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
                    # If it was pre-existing in DB, we DON'T subtract because DB has it and we want to match DB
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
