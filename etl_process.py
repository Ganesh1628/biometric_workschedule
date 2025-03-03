import pandas as pd
import pyodbc
import sqlalchemy
import logging
from sqlalchemy import text
from datetime import timedelta

# Configure Logging
logging.basicConfig(filename="etl_log.log", level=logging.INFO, 
                    format="%(asctime)s - %(levelname)s - %(message)s")

# MSSQL Database Credentials
MSSQL_CONFIG = {
    "DRIVER": "{SQL Server}",
    "SERVER": "DELL7415\\SQLEXP2019",
    "DATABASE": "LHRM0001",
    "USER": "sa",
    "PASSWORD": "your_password"
}

# POSTGRES_CONFIG = {
#     "DATABASE": "ems",  
#     "USER": "postgres",
#     "PASSWORD": "igs-ems",
#     "HOST": "3.226.14.5",
#     "PORT": "5433"
# }

# SQLAlchemy connection strings
SQLALCHEMY_MSSQL_URI = (
    f"mssql+pyodbc://{MSSQL_CONFIG['SERVER']}/{MSSQL_CONFIG['DATABASE']}?driver=SQL+Server&Trusted_Connection=yes"
)
SQLALCHEMY_POSTGRES_URI = f"postgresql+psycopg2://{POSTGRES_CONFIG['USER']}:{POSTGRES_CONFIG['PASSWORD']}@{POSTGRES_CONFIG['HOST']}:{POSTGRES_CONFIG['PORT']}/{POSTGRES_CONFIG['DATABASE']}"

# Connect to MSSQL
def connect_mssql():
    try:
        conn_str = f"DRIVER={MSSQL_CONFIG['DRIVER']};SERVER={MSSQL_CONFIG['SERVER']};DATABASE={MSSQL_CONFIG['DATABASE']};Trusted_Connection=yes;"
        conn = pyodbc.connect(conn_str)
        return conn
    except Exception as e:
        logging.error(f"MSSQL Connection Error: {e}")
        return None

# Create SQLAlchemy Engines
def get_sqlalchemy_engine_mssql():
    try:
        return sqlalchemy.create_engine(SQLALCHEMY_MSSQL_URI)
    except Exception as e:
        logging.error(f"SQLAlchemy MSSQL Connection Error: {e}")
        return None

def get_sqlalchemy_engine_postgres():
    try:
        return sqlalchemy.create_engine(SQLALCHEMY_POSTGRES_URI)
    except Exception as e:
        logging.error(f"SQLAlchemy PostgreSQL Connection Error: {e}")
        return None

# Fetch Employee Attendance Data
def fetch_employee_attendance():
    engine = get_sqlalchemy_engine_mssql()
    if not engine:
        return None

    query = """
    SELECT ed.EmpIdN, ed.EmpNameC, e.AttdDateD, e.NWHN
    FROM dbo.EmpMaster ed
    INNER JOIN dbo.EmpDailyAttd e 
        ON LTRIM(RTRIM(CAST(ed.EmpIdN AS NVARCHAR))) = LTRIM(RTRIM(CAST(e.EmpIdN AS NVARCHAR)))
    """

    df = pd.read_sql(query, engine)
    engine.dispose()

    # Debugging
    print("Fetched DataFrame Columns:", df.columns)
    # print(df.dtypes)  # Print column data types
    print("Sample Data:\n", df.head())  # Print sample data

    if df.empty:
        logging.warning("No data fetched from the MSSQL database.")
    
    df['AttdDateD'] = pd.to_datetime(df['AttdDateD'], errors='coerce')  # Convert to datetime
    print("?????????????", df)
    return df

# Compare EmpNameC with User_employee and fetch id
def get_employee_ids(df):
    engine = get_sqlalchemy_engine_postgres()
    if not engine:
        return None

    query = """
    SELECT id, employee_name FROM "User_employee"
    """
    postgres_df = pd.read_sql(query, engine)
    engine.dispose()
    
    # Debugging: Check the PostgreSQL data
    print("PostgreSQL DataFrame Columns:", postgres_df.columns)
    print("PostgreSQL Sample Data:\n", postgres_df.head())
    # Merge with case-insensitive matching
    df["EmpNameC"] = df["EmpNameC"].str.lower()
    postgres_df["employee_name"] = postgres_df["employee_name"].str.lower()

    merged_df = df.merge(postgres_df, left_on="EmpNameC", right_on="employee_name", how="left")

    return merged_df[['EmpIdN', 'EmpNameC', 'AttdDateD', 'NWHN', 'id']]

def check_and_insert_biometric_schedule(df):
    engine = get_sqlalchemy_engine_postgres()
    if not engine:
        return None

    # Fetch existing biometric schedule
    query = """
    SELECT employee_id, start_date, end_date FROM biometric_workschedule
    """
    biometric_df = pd.read_sql(query, engine)

    # Convert columns to datetime
    biometric_df['start_date'] = pd.to_datetime(biometric_df['start_date'], errors='coerce')
    biometric_df['end_date'] = pd.to_datetime(biometric_df['end_date'], errors='coerce')
    df['AttdDateD'] = pd.to_datetime(df['AttdDateD'], errors='coerce')

    # Ensure 'id' column is present in df and rename it to 'employee_id' for the join
    if 'id' not in df.columns:
        logging.error("'id' column is missing in the DataFrame. Cannot proceed with merge.")
        return None

    df.rename(columns={'id': 'employee_id'}, inplace=True)

    # Filter out rows where employee_id is NaN
    if df['employee_id'].isna().any():
        logging.warning("Some rows have missing employee_id. These rows will be skipped.")
        df = df.dropna(subset=['employee_id'])

    # Merge data on 'employee_id'
    merged_df = df.merge(biometric_df, on='employee_id', how='left')

    # Filter rows where attendance date is NOT within the range
    missing_schedule_df = merged_df[
        ~((merged_df['AttdDateD'] >= merged_df['start_date']) & 
        (merged_df['AttdDateD'] <= merged_df['end_date']))
    ]

    # Insert new rows for missing schedules
    for index, row in missing_schedule_df.iterrows():
        attd_date = row['AttdDateD']
        
        # Calculate start_date (Monday of the week) and end_date (Sunday of the week)
        start_date = attd_date - timedelta(days=attd_date.weekday())  # Monday
        end_date = start_date + timedelta(days=6)  # Sunday

        # Check if a row already exists for this employee and week
        existing_row_query = text(f"""
            SELECT * FROM biometric_workschedule
            WHERE employee_id = :employee_id
            AND start_date = :start_date
            AND end_date = :end_date
        """)
        with engine.connect() as conn:
            existing_row = conn.execute(existing_row_query, {
                'employee_id': int(row['employee_id']),  # Ensure employee_id is an integer
                'start_date': start_date,
                'end_date': end_date
            }).fetchone()

        # If no row exists, insert a new row
        if not existing_row:
            insert_query = text(f"""
                INSERT INTO biometric_workschedule (
                    start_date, end_date, employee_id,
                    loginhour_monday, loginhour_tuesday, loginhour_wednesday,
                    loginhour_thursday, loginhour_friday, loginhour_saturday,
                    loginhour_sunday, total_login_hour
                )
                VALUES (
                    :start_date, :end_date, :employee_id,
                    0, 0, 0, 0, 0, 0, 0, 0
                )
            """)
            print(f"Inserting new row: start_date={start_date}, end_date={end_date}, employee_id={int(row['employee_id'])}")
            with engine.connect() as conn:
                conn.execute(insert_query, {
                    'start_date': start_date,
                    'end_date': end_date,
                    'employee_id': int(row['employee_id'])  # Ensure employee_id is an integer
                })
                conn.commit()

    engine.dispose()

    return merged_df
    engine = get_sqlalchemy_engine_postgres()
    if not engine:
        return None

    # Fetch existing biometric schedule
    query = """
    SELECT employee_id, start_date, end_date FROM biometric_workschedule
    """
    biometric_df = pd.read_sql(query, engine)

    # Convert columns to datetime
    biometric_df['start_date'] = pd.to_datetime(biometric_df['start_date'], errors='coerce')
    biometric_df['end_date'] = pd.to_datetime(biometric_df['end_date'], errors='coerce')
    df['AttdDateD'] = pd.to_datetime(df['AttdDateD'], errors='coerce')

    # Ensure 'id' column is present in df and rename it to 'employee_id' for the join
    if 'id' not in df.columns:
        logging.error("'id' column is missing in the DataFrame. Cannot proceed with merge.")
        return None

    df.rename(columns={'id': 'employee_id'}, inplace=True)

    # Merge data on 'employee_id'
    merged_df = df.merge(biometric_df, on='employee_id', how='left')

    # Filter rows where attendance date is NOT within the range
    missing_schedule_df = merged_df[
        ~((merged_df['AttdDateD'] >= merged_df['start_date']) & 
        (merged_df['AttdDateD'] <= merged_df['end_date']))
    ]

    # Insert new rows for missing schedules
    for index, row in missing_schedule_df.iterrows():
        attd_date = row['AttdDateD']
        
        # Calculate start_date (Monday of the week) and end_date (Sunday of the week)
        start_date = attd_date - timedelta(days=attd_date.weekday())  # Monday
        end_date = start_date + timedelta(days=6)  # Sunday

        # Check if a row already exists for this employee and week
        existing_row_query = text(f"""
            SELECT * FROM biometric_workschedule
            WHERE employee_id = :employee_id
            AND start_date = :start_date
            AND end_date = :end_date
        """)
        with engine.connect() as conn:
            existing_row = conn.execute(existing_row_query, {
                'employee_id': row['employee_id'],
                'start_date': start_date,
                'end_date': end_date
            }).fetchone()

        # If no row exists, insert a new row
        if not existing_row:
            insert_query = text(f"""
                INSERT INTO biometric_workschedule (
                    start_date, end_date, employee_id,
                    loginhour_monday, loginhour_tuesday, loginhour_wednesday,
                    loginhour_thursday, loginhour_friday, loginhour_saturday,
                    loginhour_sunday, total_login_hour
                )
                VALUES (
                    :start_date, :end_date, :employee_id,
                    0, 0, 0, 0, 0, 0, 0, 0
                )
            """)
            print(f"Inserting new row: start_date={start_date}, end_date={end_date}, employee_id={row['employee_id']}")
            with engine.connect() as conn:
                conn.execute(insert_query, {
                    'start_date': start_date,
                    'end_date': end_date,
                    'employee_id': row['employee_id']
                })
                conn.commit()

    engine.dispose()

    return merged_df

def update_postgres_with_nwhn(df):
    engine = get_sqlalchemy_engine_postgres()
    if not engine:
        return None

    # Map days of the week to the corresponding PostgreSQL columns
    day_to_column = {
        0: "loginhour_monday",
        1: "loginhour_tuesday",
        2: "loginhour_wednesday",
        3: "loginhour_thursday",
        4: "loginhour_friday",
        5: "loginhour_saturday",
        6: "loginhour_sunday"
    }

    # Extract the day of the week from AttdDateD
    df['day_of_week'] = df['AttdDateD'].dt.dayofweek

    # Iterate through the DataFrame and update PostgreSQL
    for index, row in df.iterrows():
        attd_date = row['AttdDateD']
        start_date = attd_date - timedelta(days=attd_date.weekday())  # Monday of the week
        end_date = start_date + timedelta(days=6)  # Sunday of the week

        day_column = day_to_column.get(row['day_of_week'])
        if day_column:
            update_query = text(f"""
                UPDATE biometric_workschedule
                SET {day_column} = :nwhn
                WHERE employee_id = :employee_id
                AND start_date = :start_date
                AND end_date = :end_date
            """)
            print(f"Executing Query: {update_query} with params: {row['NWHN'], row['employee_id'], start_date, end_date}")
            with engine.connect() as conn:
                conn.execute(update_query, {
                    'nwhn': row['NWHN'],
                    'employee_id': row['employee_id'],
                    'start_date': start_date,
                    'end_date': end_date
                })
                conn.commit()

    engine.dispose()
# Run ETL Process
def run_etl():
    employee_attendance_df = fetch_employee_attendance()
    if employee_attendance_df is None or employee_attendance_df.empty:
        logging.error("Failed to fetch data or data is empty. ETL process stopped.")
        return
    
    employee_ids_df = get_employee_ids(employee_attendance_df)
    if employee_ids_df is None or employee_ids_df.empty:
        logging.error("Failed to map Employee Names to IDs or data is empty. ETL process stopped.")
        return
    
    final_df = check_and_insert_biometric_schedule(employee_ids_df)
    if final_df is None or final_df.empty:
        logging.error("Failed to check biometric schedule or data is empty. ETL process stopped.")
        return
    
    # Print Extracted Data
    print("\nFinal DataFrame:")
    print(final_df)

    # Print NWHN values
    if 'NWHN' in final_df.columns:
        print("\nExtracted NWHN Values:")
        print(final_df['NWHN'])
    else:
        logging.warning("'NWHN' column not found in the final DataFrame.")

    # Update PostgreSQL with NWHN values
    update_postgres_with_nwhn(final_df)

    logging.info("ETL Process Completed Successfully")

if __name__ == "__main__":
    run_etl()
