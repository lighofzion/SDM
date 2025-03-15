import pandas as pd
import re
import datetime
from pathlib import Path
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import os

# PostgreSQL connection details
DB_USER = os.getenv('DB_USER')  # e.g., postgres
DB_PASSWORD = os.getenv('DB_PASSWORD')  # e.g., password
DB_HOST = os.getenv('DB_HOST')  # e.g., localhost or IP address
DB_PORT = os.getenv('DB_PORT')  # e.g., 5432
DB_NAME = os.getenv('DB_NAME')

# Create SQLAlchemy engine
engine = create_engine(f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Replace 'your_file.csv' with the actual path to your CSV file
current_dir = Path(__file__).resolve().parent.parent.parent.parent
data_folder = current_dir / "data" / "March_1"
file_path = data_folder / 'march_daniel_issac_gsn.csv'
print(f"Using file path: {file_path}")

# Extract just the filename for tracking
file_name = file_path.name
print(f"File name: {file_name}")

# Ask user how many rows to ignore from the end
ignore_rows = int(input("Enter the number of rows to ignore from the end: ") or 0)
header_row = int(input("Enter the row number where the header is located (starting from 1): ") or 3) - 1

# Ask user to select class from a predefined list
class_options = ["Class A", "Class B", "Class C", "Other"]
print("Select the class name:")
for i, option in enumerate(class_options, 1):
    print(f"{i}. {option}")
class_choice = int(input("Enter the number corresponding to your choice: ") or 1)
class_name = class_options[class_choice - 1] if 1 <= class_choice <= len(class_options) else "Other"

# Read the CSV file with specified header row
df = pd.read_csv(file_path, header=header_row)

# Drop the last 'ignore_rows' rows if specified
if ignore_rows > 0:
    df = df.iloc[:-ignore_rows]

# Display all columns in the dataframe
print("Columns in the CSV file:")
print(df.columns.tolist())

# Define patterns to search for specific columns
patterns = {
    "student_name": re.compile(r'name|student', re.IGNORECASE),
    "mobile_phone": re.compile(r'mobile|phone', re.IGNORECASE),
    "staff": re.compile(r'staff', re.IGNORECASE),
    "networker": re.compile(r'networker', re.IGNORECASE)
}

# Find matching columns
matched_columns = {}
for key, pattern in patterns.items():
    for column in df.columns:
        if pattern.search(column):
            matched_columns[key] = column
            break

print("Matched Columns:")
print(matched_columns)

# Keep only matched columns
df = df[[col for col in matched_columns.values() if col in df.columns]]

# Print original columns before renaming
print("Original columns before renaming:")
print(df.columns.tolist())

# Rename columns to match database schema
column_mapping = {matched_columns.get(key, ''): key for key in patterns.keys() if matched_columns.get(key, '') in df.columns}
df = df.rename(columns=column_mapping)

# Print columns after renaming
print("Columns after renaming:")
print(df.columns.tolist())

# Normalize mobile numbers and determine country if a matching column exists
if "mobile_phone" in df.columns:
    # Convert to string and clean up non-numeric characters
    df["mobile_phone"] = df["mobile_phone"].astype(str).str.replace(r'\D', '', regex=True)
    
    # Replace empty strings and 'nan' with None/NaN
    df.loc[df["mobile_phone"].isin(["", "nan"]), "mobile_phone"] = None
    
    # Determine country only for valid phone numbers
    def get_country(phone):
        if pd.isna(phone) or phone is None or phone == "":
            return None
        if len(str(phone)) == 10:
            return "India"
        elif len(str(phone)) == 9 and str(phone).startswith("7"):
            return "Sri Lanka"
        elif str(phone).startswith("94") and len(str(phone)) == 11:
            return "Sri Lanka"
        else:
            return "Others"
    
    df["country"] = df["mobile_phone"].apply(get_country)

# Add class name and ETL metadata columns
df["class"] = class_name
df["source_file"] = file_name  # Add source file name column
df["etl_insert_dttm"] = datetime.datetime.utcnow().isoformat()
df["etl_update_dttm"] = None  # Will be updated for records that are changed
df["etl_user_id"] = "system"  # Change as needed

try:
    # Print the data types and first few rows of the dataframe for debugging
    print("\nDataFrame dtypes:")
    print(df.dtypes)
    print("\nFirst few rows of DataFrame:")
    print(df.head())
    
    # Create a connection to check if the table exists and get existing records
    with engine.connect() as connection:
        # Check if the table exists
        table_exists = connection.execute(text(
            "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'iba' AND table_name = 'students')"
        )).scalar()
        
        if not table_exists:
            # Create the table if it doesn't exist - make mobile_phone UNIQUE but allow NULL values
            connection.execute(text("""
                CREATE SCHEMA IF NOT EXISTS iba;
                
                CREATE TABLE IF NOT EXISTS iba.students (
                    id SERIAL PRIMARY KEY,
                    student_name TEXT,
                    mobile_phone TEXT,
                    staff TEXT,
                    networker TEXT,
                    country TEXT,
                    class TEXT,
                    source_file TEXT,
                    etl_insert_dttm TIMESTAMP,
                    etl_update_dttm TIMESTAMP,
                    etl_user_id TEXT,
                    CONSTRAINT students_mobile_phone_key UNIQUE (mobile_phone)
                )
            """))
            connection.commit()
        
        # Get existing records to check for updates and avoid duplicates
        existing_records = {}
        result = connection.execute(text("""
            SELECT id, student_name, mobile_phone, staff, networker, country, class, source_file 
            FROM iba.students 
            WHERE mobile_phone IS NOT NULL
        """))
        
        for row in result:
            # Use mobile_phone as key for lookup
            if row[2]:  # mobile_phone is not None
                existing_records[row[2]] = {
                    'id': row[0],
                    'student_name': row[1],
                    'mobile_phone': row[2],
                    'staff': row[3],
                    'networker': row[4],
                    'country': row[5],
                    'class': row[6],
                    'source_file': row[7]
                }
    
    # Print for debugging
    print(f"Found {len(existing_records)} existing records in database")
    
    # Prepare dataframes for insert and update operations
    records_to_insert = []
    records_to_update = []
    
    # Process each record in the DataFrame
    for _, row in df.iterrows():
        # Skip records with no mobile_phone
        if pd.isna(row.get('mobile_phone')) or row.get('mobile_phone') is None or row.get('mobile_phone') == '':
            continue
        
        mobile = str(row['mobile_phone'])
        
        # Check if this record exists in the database
        if mobile in existing_records:
            existing = existing_records[mobile]
            changes = {}
            
            # Check for changes in each field (only collect actual changes)
            for field in ['student_name', 'staff', 'networker', 'country', 'class', 'source_file']:
                if field in row:
                    # Handle None/NaN vs empty string equivalence
                    existing_value = existing[field] if existing[field] is not None else ''
                    current_value = row[field] if not pd.isna(row[field]) and row[field] is not None else ''
                    
                    # Only mark as changed if values are actually different
                    if str(existing_value).strip() != str(current_value).strip():
                        changes[field] = current_value
            
            # Only add to update list if there are actual changes
            if changes:
                changes['id'] = existing['id']
                changes['mobile_phone'] = mobile  # Include mobile_phone for reference
                changes['etl_update_dttm'] = datetime.datetime.utcnow().isoformat()
                changes['etl_user_id'] = 'system'
                records_to_update.append(changes)
        else:
            # New record, add to insert list
            record = row.to_dict()
            # Ensure all required columns exist
            for col in ['student_name', 'mobile_phone', 'staff', 'networker', 'country', 'class', 
                        'source_file', 'etl_insert_dttm', 'etl_update_dttm', 'etl_user_id']:
                if col not in record:
                    record[col] = None
            records_to_insert.append(record)
    
    # Create a dataframe for insertions
    if records_to_insert:
        df_to_insert = pd.DataFrame(records_to_insert)
        print(f"{len(df_to_insert)} new records will be inserted")
    else:
        df_to_insert = pd.DataFrame()
        print("No new records to insert")
    
    # Create a dataframe for updates
    if records_to_update:
        df_to_update = pd.DataFrame(records_to_update)
        print(f"{len(df_to_update)} existing records will be updated")
        print("Sample of records to be updated:")
        print(df_to_update.head())
    else:
        df_to_update = pd.DataFrame()
        print("No records to update")
    
    # Perform database operations
    with engine.begin() as connection:
        # Insert new records
        if not df_to_insert.empty:
            # Only keep the columns that exist in the database table
            required_columns = ['student_name', 'mobile_phone', 'staff', 'networker', 
                                'country', 'class', 'source_file', 'etl_insert_dttm', 
                                'etl_update_dttm', 'etl_user_id']
            df_to_insert = df_to_insert[[col for col in required_columns if col in df_to_insert.columns]]
            
            df_to_insert.to_sql('students', connection, schema='iba', if_exists='append', index=False)
            print(f"{len(df_to_insert)} new records inserted successfully")
        
        # Update existing records
        if not df_to_update.empty:
            # Generate and execute update statements
            update_count = 0
            fields_updated = {field: 0 for field in ['student_name', 'staff', 'networker', 'country', 'class', 'source_file']}
            
            for _, row in df_to_update.iterrows():
                update_fields = []
                params = {'id': row['id']}
                
                # Build the SET clause for each field that needs updating
                for field in ['student_name', 'staff', 'networker', 'country', 'class', 'source_file']:
                    if field in row and not pd.isna(row[field]):
                        update_fields.append(f"{field} = :{field}")
                        params[field] = row[field]
                        fields_updated[field] += 1
                
                # Add mandatory update metadata
                update_fields.append("etl_update_dttm = :etl_update_dttm")
                update_fields.append("etl_user_id = :etl_user_id")
                params['etl_update_dttm'] = row['etl_update_dttm']
                params['etl_user_id'] = row['etl_user_id']
                
                # Execute the update statement
                if update_fields:
                    update_stmt = text(f"""
                        UPDATE iba.students 
                        SET {', '.join(update_fields)} 
                        WHERE id = :id
                    """)
                    
                    connection.execute(update_stmt, params)
                    update_count += 1
            
            print(f"{update_count} records updated successfully")
            
            # Print breakdown of which fields were updated
            print("Field update breakdown:")
            for field, count in fields_updated.items():
                if count > 0:
                    print(f"  - {field}: {count} updates")
    
    print("\nETL operation completed successfully")
    
except SQLAlchemyError as e:
    print(f"Database error: {e}")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    # Close the connection pool
    engine.dispose()