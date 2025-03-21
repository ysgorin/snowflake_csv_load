# csv_load.py

# Read CSV File

# Dependencies
import pandas as pd
from collections import defaultdict
from snowflake_creds import creds
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import sys

def main():
    csv_data = input('Enter path to the CSV file: ')

    # Read csv into a pandas DataFrame
    try:
        df = pd.read_csv(csv_data,
                        dtype=defaultdict(lambda: str))
    except FileNotFoundError:
        print('Error: File not found.')
        sys.exit(1) # Use error exit code

    # Get table details
    database_name = input('Enter database name: ').upper()
    schema_name = input('Enter schema name: ').upper()
    table_name = input('Enter table name: ').upper()
    
    # Write CREATE TABLE statement
    table_cols = ',\n    '.join([f'"{col}" VARCHAR' for col in df.columns])

    create_table_sql = f"""CREATE OR REPLACE TABLE {table_name} (
        {table_cols}
    );"""

    print('Create table statement:', create_table_sql)
    if input('Review the create table statment. Continue? y/n ') != 'y':
        sys.exit(0) # Use no errors exit code

    # Connect to Snowflake
    connection_parameters = {
        'user': creds['user'],
        'password': creds['password'],
        'account': creds['account'],
        'warehouse': creds['warehouse'],
        'database': database_name,
        'schema': schema_name
    }

    con = None
    cur = None
    
    try:
        con = snowflake.connector.connect(**connection_parameters)

        cur = con.cursor()

        # Execute create table statement
        cur.execute(create_table_sql)

        # Load data into created table
        success, nchunks, nrows, _ = write_pandas(con, df, table_name)
        print(f"Success: {success}, Chunks: {nchunks}, Rows Inserted: {nrows}")
    except snowflake.connector.errors.DatabaseError as e:
        print(f'Database error: {e}')
    except Exception as e:
        print(f'Unexpected error: {e}')
    finally:
        if cur:
            cur.close()
        if con:
            con.close()

if __name__ == '__main__':
    main()