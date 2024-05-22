import subprocess
import time
import re
import csv
from io import StringIO
import pandas as pd

def wait_for_postgres(host, max_retries=5, delay_seconds=5):
    """Wait for PostgreSQL to become available."""
    retries = 0
    while retries < max_retries:
        try:
            result = subprocess.run(
                ["pg_isready", "-h", host], check=True, capture_output=True, text=True)
            if "accepting connections" in result.stdout:
                print("Successfully connected to PostgreSQL!")
                return True
        except subprocess.CalledProcessError as e:
            print(f"Error connecting to PostgreSQL: {e}")
            retries += 1
            print(
                f"Retrying in {delay_seconds} seconds... (Attempt {retries}/{max_retries})")
            time.sleep(delay_seconds)
    print("Max retries reached. Exiting.")
    return False

def str_to_bool(value):
    if value is None:
        return None
    return value.lower() == 'yes'

def transform_row(row):
    """Transform a single row according to your mapping logic."""
    try:
        total_charges = float(row["Total Charges"]) if row["Total Charges"] not in [None, ''] else None
    except ValueError:
        total_charges = None
    
    return {
        'customerID': str(row["CustomerID"]) if row["CustomerID"] is not None else None,
        'count': int(row["Count"]) if row["Count"] is not None else None,
        'country': str(row["Country"]) if row["Country"] is not None else None,
        'state': str(row["State"]) if row["State"] is not None else None,
        'city': str(row["City"]) if row["City"] is not None else None,
        'zipCode': int(row["Zip Code"]) if row["Zip Code"] is not None else None,
        'latlong': str(row["Lat Long"]) if row["Lat Long"] is not None else None,
        'latitude': float(row["Latitude"]) if row["Latitude"] is not None else None,
        'longitude': float(row["Longitude"]) if row["Longitude"] is not None else None,
        'gender': str(row["Gender"]) if row["Gender"] is not None else None,
        'seniorCitizen_fl': str_to_bool(row["Senior Citizen"]),
        'partner': str_to_bool(row["Partner"]),
        'dependents': str_to_bool(row["Dependents"]),
        'tenure_months': int(row["Tenure Months"]) if row["Tenure Months"] is not None else None,
        'phoneService': str_to_bool(row["Phone Service"]),
        'multipleLines': str(row["Multiple Lines"]) if row["Multiple Lines"] is not None else None,
        'internetService': str(row["Internet Service"]) if row["Internet Service"] is not None else None,
        'onlineSecurity': str(row["Online Security"]) if row["Online Security"] is not None else None,
        'onlineBackup': str(row["Online Backup"]) if row["Online Backup"] is not None else None,
        'deviceProtection': str(row["Device Protection"]) if row["Device Protection"] is not None else None,
        'techSupport': str(row["Tech Support"]) if row["Tech Support"] is not None else None,
        'streamingTV': str(row["Streaming TV"]) if row["Streaming TV"] is not None else None,
        'streamingMovies': str(row["Streaming Movies"]) if row["Streaming Movies"] is not None else None,
        'contract': str(row["Contract"]) if row["Contract"] is not None else None,
        'paperlessBilling': str_to_bool(row["Paperless Billing"]),
        'paymentMethod': str(row["Payment Method"]) if row["Payment Method"] is not None else None,
        'monthlyCharges': float(row["Monthly Charges"]) if row["Monthly Charges"] is not None else None,
        'totalCharges': total_charges,
        'churn_label': str_to_bool(row["Churn Label"]),
        'churn_value': int(row["Churn Value"]) if row["Churn Value"] is not None else None,
        'churn_score': int(row["Churn Score"]) if row["Churn Score"] is not None else None,
        'cltv': int(row["CLTV"]) if row["CLTV"] is not None else None,
        'churn_reason': str(row["Churn Reason"]).replace("Don't know", "Unknown") if row["Churn Reason"] is not None else None
    }

def parse_insert_statements(sql_content):
    """Parse the SQL content to extract INSERT statements."""
    insert_statements = re.findall(r"INSERT INTO .*? VALUES .*?;", sql_content, re.DOTALL)
    return insert_statements

def transform_insert_statement(insert_statement):
    """Transform the data within an INSERT statement."""
    # Extract table name and values
    match = re.match(r"(INSERT INTO .*? VALUES )(\(.*?\));", insert_statement, re.DOTALL)
    if not match:
        return insert_statement

    preamble, values_part = match.groups()
    values_part = values_part.strip("();")
    rows = [row.strip() for row in values_part.split("),(")]

    # Define columns manually or extract from SQL (assuming order matches `transform_row`)
    columns = ["CustomerID", "Count", "Country", "State", "City", "Zip Code", "Lat Long", 
               "Latitude", "Longitude", "Gender", "Senior Citizen", "Partner", "Dependents", 
               "Tenure Months", "Phone Service", "Multiple Lines", "Internet Service", 
               "Online Security", "Online Backup", "Device Protection", "Tech Support", 
               "Streaming TV", "Streaming Movies", "Contract", "Paperless Billing", 
               "Payment Method", "Monthly Charges", "Total Charges", "Churn Label", 
               "Churn Value", "Churn Score", "CLTV", "Churn Reason"]

    # Transform rows
    transformed_rows = []
    for row in rows:
        row_values = list(csv.reader(StringIO(row)))[0]
        row_dict = dict(zip(columns, row_values))
        transformed_row = transform_row(row_dict)
        transformed_row_values = ', '.join(
            [f"'{v}'" if isinstance(v, str) else str(v) for v in transformed_row.values()])
        transformed_rows.append(f"({transformed_row_values})")

    transformed_values_part = ", ".join(transformed_rows)
    return f"{preamble}({transformed_values_part});"

def modify_sql_file(file_path):
    """Modify data types and values in the SQL file."""
    with open(file_path, 'r') as file:
        sql_content = file.read()

    insert_statements = parse_insert_statements(sql_content)
    for statement in insert_statements:
        transformed_statement = transform_insert_statement(statement)
        sql_content = sql_content.replace(statement, transformed_statement)

    with open(file_path, 'w') as file:
        file.write(sql_content)

# Use the function before running the ELT process
if not wait_for_postgres(host="source_postgres"):
    exit(1)

print("Starting ELT script...")

# Configuration for the source PostgreSQL database
source_config = {
    'dbname': 'source_db',
    'user': 'postgres',
    'password': 'secret',
    # Use the service name from docker-compose as the hostname
    'host': 'source_postgres'
}

# Configuration for the destination PostgreSQL database
destination_config = {
    'dbname': 'destination_db',
    'user': 'postgres',
    'password': 'secret',
    # Use the service name from docker-compose as the hostname
    'host': 'destination_postgres'
}

# Use pg_dump to dump the source database to a SQL file
dump_command = [
    'pg_dump',
    '-h', source_config['host'],
    '-U', source_config['user'],
    '-d', source_config['dbname'],
    '-f', 'data_dump.sql',
    '-w'  # Do not prompt for password
]

# Set the PGPASSWORD environment variable to avoid password prompt
subprocess_env = dict(PGPASSWORD=source_config['password'])

print("Extracting from source db")

# Execute the dump command
subprocess.run(dump_command, env=subprocess_env, check=True)

print("Extracted!")

print("Performing source-target mapping")

# Modify the SQL file
modify_sql_file('data_dump.sql')

print("Mapped!")

# Use psql to load the modified SQL file into the destination database
load_command = [
    'psql',
    '-h', destination_config['host'],
    '-U', destination_config['user'],
    '-d', destination_config['dbname'],
    '-a', '-f', 'data_dump.sql'
]

# Set the PGPASSWORD environment variable for the destination database
subprocess_env = dict(PGPASSWORD=destination_config['password'])

print("Loading data to destination db")

# Execute the load command
subprocess.run(load_command, env=subprocess_env, check=True)

print("Loaded!")

def fetch_data_from_postgres(destination_config):
    """Fetch data from the PostgreSQL source database."""
    query = "SELECT * FROM telecom_customer_churn;" # Replace with your actual table name
    fetch_command = [
    'psql',
    '-h', destination_config['host'],
    '-U', destination_config['user'],
    '-d', destination_config['dbname'],
    '-c', query,
    '-A', '-F', ',' # Output as CSV
    ]

    # Set the PGPASSWORD environment variable to avoid password prompt
    subprocess_env = dict(PGPASSWORD=source_config['password'])

    result = subprocess.run(fetch_command, env=subprocess_env, capture_output=True, text=True, check=True)
    return pd.read_csv(StringIO(result.stdout))


print("Ending ELT script...")
