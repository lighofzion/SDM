import os
import warnings
from datetime import datetime
import asyncio
import nest_asyncio
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, DateTime, or_
from sqlalchemy.dialects.postgresql import insert
from fuzzywuzzy import process
from googletrans import Translator

# Suppress warnings and enable nested asyncio
warnings.filterwarnings('ignore')
nest_asyncio.apply()

def translate_sync(text, translator, src='ko', dest='en'):
    """
    Synchronously translate text using the asynchronous translator.
    """
    loop = asyncio.get_event_loop()
    result = loop.run_until_complete(translator.translate(text, src=src, dest=dest))
    return result

def read_spreadsheet_with_fuzzy_matching(file_path, sheet_name=None, target_columns=None):
    """
    Read spreadsheet data with fuzzy column name matching, automatically translating
    any Korean column names to English, and stopping at the first completely empty row.
    
    Parameters:
        file_path: Path object for the spreadsheet file.
        sheet_name: Name of the sheet to read (for Excel files).
        target_columns: List of target column names to match.
        
    Returns:
        pandas DataFrame with selected data.
    """
    file_ext = file_path.suffix.lower()

    # Read file with header (assumed to be in the first row)
    if file_ext in ['.xlsx', '.xls']:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
    elif file_ext == '.csv':
        df = pd.read_csv(file_path)
    else:
        raise ValueError(f"Unsupported file format: {file_ext}")

    # Stop at the first completely empty row
    empty_mask = df.isnull().all(axis=1)
    if empty_mask.any():
        first_empty_index = empty_mask.idxmax()
        print(f"First completely empty row detected at index {first_empty_index}")
        df = df.iloc[:first_empty_index]
    else:
        print("No empty rows detected, using all data")
    
    # Translate column names containing Korean characters
    translator = Translator()
    new_columns = {}
    for col in df.columns:
        if any('\uac00' <= ch <= '\ud7a3' for ch in str(col)):
            try:
                result = translate_sync(col, translator, src='ko', dest='en')
                new_columns[col] = result.text
                print(f"Translated '{col}' to '{result.text}'")
            except Exception as e:
                print(f"Error translating column '{col}': {e}. Keeping original.")
                new_columns[col] = col
        else:
            new_columns[col] = col
    df.rename(columns=new_columns, inplace=True)
    
    # Normalize column names: lowercase, strip spaces, replace spaces with underscores
    df.columns = df.columns.str.lower().str.strip().str.replace(" ", "_")

    if not target_columns:
        return df

    # Fuzzy match target columns with the actual DataFrame columns
    matched_columns = {}
    actual_columns = df.columns.tolist()
    for target in target_columns:
        best_match, score = process.extractOne(target, actual_columns)
        print(f"Matched target '{target}' to column '{best_match}' with score {score}")
        matched_columns[target] = best_match

    selected_columns = list(matched_columns.values())
    if len(selected_columns) < len(target_columns):
        print("Warning: Could not find matches for all target columns")
    
    result_df = df.loc[:, selected_columns].copy()

    # Replace forward slashes with dots in the "name" column if it exists
    name_column = matched_columns.get("name")
    if name_column in result_df.columns:
        result_df.loc[:, name_column] = result_df[name_column].str.replace('/', '.', regex=False)
    
    return result_df

def create_table_if_not_exists(engine, schema_name, table_name, df):
    """
    Create the target table if it doesn't exist.
    For simplicity, all columns are mapped as String except our ETL columns (inserted_at, updated_at) which are DateTime.
    The 'scj_number' column is used as the primary key.
    """
    metadata = MetaData(schema=schema_name)
    columns = []
    for col in df.columns:
        if col in ('inserted_at', 'updated_at'):
            columns.append(Column(col, DateTime))
        else:
            columns.append(Column(col, String))
    # Set primary key on 'scj_number'
    for col in columns:
        if col.name == 'scj_number':
            col.primary_key = True
    table = Table(table_name, metadata, *columns)
    metadata.create_all(engine, checkfirst=True)
    print(f"Ensured table '{schema_name}.{table_name}' exists with proper schema.")

def upsert_data(engine, schema_name, table_name, df):
    """
    Upsert records into the table based on the primary key 'scj_number'.
    
    ETL logic:
      - For new records, both inserted_at and updated_at are set to the current timestamp.
      - For existing records, compare all non-ETL columns.
          - If there are differences, update the record (update updated_at only, preserve inserted_at).
          - If there are no differences, leave the record unchanged.
    """
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine, schema=schema_name)
    records = df.to_dict(orient="records")
    if not records:
        print("No records to upsert.")
        return
    
    stmt = insert(table).values(records)
    # Build update mapping for columns (exclude primary key and inserted_at)
    update_dict = {col.name: getattr(stmt.excluded, col.name)
                   for col in table.columns if col.name not in ['scj_number', 'inserted_at', 'updated_at']}
    # Always update updated_at when a change is detected
    update_dict['updated_at'] = datetime.now()
    
    # Build a condition that checks if any non-ETL column is different using is_distinct_from (to handle NULLs)
    compare_conditions = []
    for col in table.columns:
        if col.name not in ['scj_number', 'inserted_at', 'updated_at']:
            compare_conditions.append(col.is_distinct_from(getattr(stmt.excluded, col.name)))
    if compare_conditions:
        from sqlalchemy import or_
        where_condition = or_(*compare_conditions)
    else:
        where_condition = None

    stmt = stmt.on_conflict_do_update(
        index_elements=['scj_number'],
        set_=update_dict,
        where=where_condition
    )
    with engine.begin() as conn:
        conn.execute(stmt)
    print(f"Upserted {len(records)} records (inserted new or updated changed ones).")

def main():
    # Build the file path dynamically: assuming the script is in a subfolder (e.g., "models") and the data folder is one level up.
    current_dir = Path(__file__).resolve().parent
    data_folder = current_dir.parent / "data"
    file_path = data_folder / "raw_data.csv"
    sheet_name = 'raw_data'
    print(f"Using file path: {file_path}")

    target_columns = [
        "scj_number", 
        "name", 
        "birth", 
        "phone", 
        "department", 
        "team", 
        "new cell grp", 
        "special note", 
        "office"
    ]

    try:
        df = read_spreadsheet_with_fuzzy_matching(file_path, sheet_name, target_columns)
    except (ValueError, KeyError) as e:
        print(f"Could not read with column names: {e}")
        return

    # Ensure the primary key column is named exactly "scj_number"
    best_match, score = process.extractOne("scj_number", df.columns.tolist())
    if best_match:
        df.rename(columns={best_match: "scj_number"}, inplace=True)
        print(f"Renamed column '{best_match}' to 'scj_number' for primary key usage.")
    else:
        print("Error: 'scj_number' column not found. Exiting.")
        return

    # Set ETL columns:
    current_ts = datetime.now()
    # For new records, inserted_at will be set. For existing records, it will be preserved.
    df['inserted_at'] = current_ts  
    df['updated_at'] = current_ts   # updated_at will be updated only if any non-ETL fields change

    print("\n=== Data Overview ===")
    print(f"Rows: {df.shape[0]}, Columns: {df.shape[1]}")
    print("\n=== Column Names ===")
    print(df.columns.tolist())
    print("\n=== Data Preview ===")
    print(df.head())

    # Database connection parameters (update these with from environment variables)
    db_params = {
        'host': os.getenv('DB_HOST'),
        'port': os.getenv('DB_PORT'),
        'dbname': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD')
    }
    schema_name = 'public'
    table_name = 'staging_data'
    full_table_name = f"{schema_name}.{table_name}"

    engine = create_engine(
        f"postgresql://{db_params['user']}:{db_params['password']}@"
        f"{db_params['host']}:{db_params['port']}/{db_params['dbname']}"
    )

    # Ensure the target table exists with our ETL columns
    create_table_if_not_exists(engine, schema_name, table_name, df)

    # Upsert new data:
    # - New records will be inserted with inserted_at and updated_at set.
    # - Existing records will be updated (if any non-ETL changes exist) and only updated_at will change.
    upsert_data(engine, schema_name, table_name, df)

    print("\n=== Summary ===")
    print(f"Processed {df.shape[0]} rows with {df.shape[1]} columns")
    print(f"Data synchronized with table {full_table_name}")

if __name__ == '__main__':
    main()
