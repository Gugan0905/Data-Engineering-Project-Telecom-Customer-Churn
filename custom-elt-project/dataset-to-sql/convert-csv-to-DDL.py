


import pandas as pd
import os

csv_file = 'Telco_customer_churn.csv'
df = pd.read_csv(csv_file)
df['Churn Reason'] = df['Churn Reason'].replace("Don't know", 'Unknown')

# Infer PostgreSQL data types from the dataframe
def map_dtype(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return 'INTEGER'
    elif pd.api.types.is_float_dtype(dtype):
        return 'DOUBLE PRECISION'
    elif pd.api.types.is_bool_dtype(dtype):
        return 'BOOLEAN'
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return 'TIMESTAMP'
    else:
        return 'TEXT'


# GENERATE SQL DDL

# CREATE TABLE statement
table_name = 'telecom_customer_churn'
create_table_stmt = f'CREATE TABLE {table_name} (\n'
for column in df.columns:
    sql_dtype = map_dtype(df[column].dtype)
    create_table_stmt += f'    "{column}" {sql_dtype},\n'
create_table_stmt = create_table_stmt.rstrip(',\n') + '\n);'

# INSERT INTO statements
insert_into_stmts = []
for index, row in df.iterrows():
    columns = ', '.join([f'"{col}"' for col in row.index])
    values = ', '.join([repr(val) if not pd.isnull(val) else 'NULL' for val in row.values])
    insert_stmt = f'INSERT INTO {table_name} ({columns}) VALUES ({values});'
    insert_into_stmts.append(insert_stmt)

# Writing all commands to sql ddl file
output_sql_file = 'init-ddl.sql'
if not os.path.exists(output_sql_file):
    print(f"Creating new file: {output_sql_file}")
else:
    print(f"Overwriting existing file: {output_sql_file}")

with open(output_sql_file, 'w') as f:
    f.write(create_table_stmt + '\n\n')
    for stmt in insert_into_stmts:
        f.write(stmt + '\n')

print(f"Successfuly converted CSV to SQL DDL in the file: {output_sql_file}")