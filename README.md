# Load CSV Files to Snowflake
### Overview
[`csv_load.py`](csv_load.py) is a Python script designed to automate the process of loading CSV data into Snowflake.</b> The script performs the following steps:
* Reads a CSV file</b>
* Generates a SQL</b> `CREATE TABLE` statement</b>
* Connects to Snowflake</b>
* Executes the </b>`CREATE TABLE` statement</b>
* Loads the CSV data into Snowflake</b>
### Requirements
* Python 3.x
* Snowflake account and credentials
* Required Python libraries
    * `pandas`
    * `snowflake-connector-python[pandas]`