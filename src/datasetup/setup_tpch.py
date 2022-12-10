import pandas as pd
import os
import sys
import subprocess
from os import path
sys.path.append(path.dirname(path.dirname( path.abspath(__file__))))
from database import psql_connectionstring, databases, TPCH_CREATE_TABLE_COMMAND, TPCH_DROP_TABLE_IF_EXIST_COMMAND
import argparse

PSQL_PATH = "C:\\Program Files\\PostgreSQL\\13\\bin\\psql.exe"
DATA_PATH = "C:\\Users\\Paul\\Desktop\\HPI\\BP\\TPC-H_Tools_v3.0.0\\dbgen\\"

data_dirs = {
    0.01 : "10MB",
    0.1 : "100MB",
    1 : "1GB"
}

def execute_sql(statement : str, connstring : str):
    subprocess.call([PSQL_PATH, '-c', statement, connstring])

master_connstring = psql_connectionstring(0)
# recreate databases
for sf in data_dirs.keys():
    dbname = databases[sf]
    print(f"Creating database {dbname}")
    execute_sql(f"DROP DATABASE IF EXISTS {dbname};", master_connstring)
    execute_sql(f"CREATE DATABASE {dbname};", master_connstring)
    

# fill with tpc-h data
for sf in data_dirs.keys():
    db_connstring = psql_connectionstring(sf)
    subprocess.call([PSQL_PATH, '-c', TPCH_DROP_TABLE_IF_EXIST_COMMAND, db_connstring])
    print(f"Creating tables for {databases[sf]}")
    execute_sql(TPCH_CREATE_TABLE_COMMAND, db_connstring)

    tables = ['orders', 'customer', 'lineitem', 'nation', 'partsupp', 'part', 'region', 'supplier']
    for table in tables: 
        print(f"{databases[sf]} : {table.capitalize()}")
        data_path = path.join(DATA_PATH, data_dirs[sf], table + ".tbl")
        command = "\\copy " + table + " FROM '" + data_path + "' with (format csv, delimiter '|');"
        execute_sql(command, db_connstring)
