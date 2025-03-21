# csv_load.py

# Read CSV File

# Dependencies
import pandas as pd
# Use defaultdict for df dtype
from collections import defaultdict
from snowflake_creds import creds
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

csv_data = input('Enter path to .csv file: ')

# Read csv into a pandas DataFrame
df = pd.read_csv(csv_data,
                 dtype=defaultdict(lambda: str))

# Write Create Statement
table_name = input('Enter table name: ').upper()
table_cols = ',\n    '.join([f'"{col}" VARCHAR' \
for col in df.columns])

create_table_sql = \
f"""CREATE OR REPLACE TABLE {table_name} (
    {table_cols}
);"""

print(create_table_sql)
move_on = input('Review the create table statment. Continue? "y" or "n"')

if move_on == 'y':
    # Connect to Snowflake
    connection_parameters = {
        'user': creds['user'],
        'password': creds['password'],
        'account': creds['account'],
        'warehouse': creds['warehouse'],
        'database': input('Enter database name: ').upper(),
        'schema': input('Enter schema name: ').upper()
    }

    con = snowflake.connector.connect(**connection_parameters)

    cur = con.cursor()

    # Execute create table statement
    cur.execute(create_table_sql)

    # Load data into created table
    success, nchunks, nrows, _ = write_pandas(con, df, table_name)
    print(f"Success: {success}, Chunks: {nchunks}, Rows Inserted: {nrows}")

    cur.close()
    con.close()