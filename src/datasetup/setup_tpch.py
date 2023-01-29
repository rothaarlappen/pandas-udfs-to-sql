import sys
import argparse
import subprocess
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from database import (
    psql_connectionstring,
    DATABASES,
    TPCH_CREATE_TABLE_COMMAND,
    TPCH_DROP_TABLE_IF_EXIST_COMMAND,
    
    SQLSERVER_HOST, SQLSERVER_PW, SQLSERVER_USER, SQLSERVER_SCALEFACTOR, SQLSERVER_SERVER
)
from typing import List

PSQL_PATH = "C:\\Program Files\\PostgreSQL\\13\\bin\\psql.exe"
SQLCMD_PATH = "C:\\Program Files\\Microsoft SQL Server\\Client SDK\\ODBC\\170\\Tools\\Binn\\SQLCMD.EXE"
DATA_PATH = "C:\\Users\\Paul\\Desktop\\HPI\\BP\\TPC-H_Tools_v3.0.0\\dbgen\\"

DATA_DIRS = {0.01: "10MB", 0.1: "100MB", 1: "1GB", 5: "5GB", 10: "10GB"}

tpch_scalefactors = DATA_DIRS.keys()
tpch_tables = [
    "orders",
    "customer",
    "lineitem",
    "nation",
    "partsupp",
    "part",
    "region",
    "supplier",
]


def execute_sql(statement: str, connstring: str):
    subprocess.call([PSQL_PATH, "-c", statement, connstring])

def execute_sqlserver_sql(statement: str, database: str):
    subprocess.call([SQLCMD_PATH, "-S", SQLSERVER_HOST, "-U", SQLSERVER_USER, "-P", SQLSERVER_PW, "-d",  database, "-Q", statement])

def setup(scalefactors: List[float], tables: List[str]):
    master_connstring = psql_connectionstring(0)
    # recreate databases doesn't work if other connections are active...
    for sf in scalefactors:
        dbname = DATABASES[sf]
        print(f"Creating database {dbname}")
        execute_sql(f"DROP DATABASE {dbname} WITH (FORCE);", master_connstring)
        execute_sql(f"CREATE DATABASE {dbname};", master_connstring)

        db_connstring = psql_connectionstring(sf)  # this database should exist now
        execute_sql(
            f"CREATE EXTENSION IF NOT EXISTS plpython3u", db_connstring
        )  # we create python extension to allow for python UDFs

    # fill with tpc-h data
    for sf in scalefactors:
        db_connstring = psql_connectionstring(sf)
        subprocess.call(
            [PSQL_PATH, "-c", TPCH_DROP_TABLE_IF_EXIST_COMMAND, db_connstring]
        )
        print(f"Creating tables for {DATABASES[sf]}")
        execute_sql(TPCH_CREATE_TABLE_COMMAND, db_connstring)

        for table in tables:
            print(f"{DATABASES[sf]} : {table.capitalize()}")
            data_path = path.join(DATA_PATH, DATA_DIRS[sf], table + ".tbl")
            command = (
                "\\copy "
                + table
                + " FROM '"
                + data_path
                + "' with (format csv, delimiter '|');"
            )
            execute_sql(command, db_connstring)

def setup_sqlserver(scalefactors: List[float], tables: List[str]):
    master_name = "master"
    # recreate databases doesn't work if other connections are active...
    for sf in scalefactors:
        dbname = DATABASES[sf]
        print(f"Creating database {dbname}")
        execute_sqlserver_sql(f"DROP DATABASE {dbname};", master_name)
        execute_sqlserver_sql(f"CREATE DATABASE {dbname};", master_name)

        db_connstring = psql_connectionstring(sf)  # this database should exist now
        execute_sqlserver_sql(
            f"sp_configure 'external scripts enabled', 1; RECONFIGURE WITH OVERRIDE;", dbname
        )  # we configure the server to allow for python UDFs

    # fill with tpc-h data
    for sf in scalefactors:
        execute_sqlserver_sql(TPCH_DROP_TABLE_IF_EXIST_COMMAND, DATABASES[sf])
        print(f"Creating tables for {DATABASES[sf]}")
        execute_sqlserver_sql(TPCH_CREATE_TABLE_COMMAND, DATABASES[sf])

        for table in tables:
            print(f"{DATABASES[sf]} : {table.capitalize()}")
            data_path = path.join(DATA_PATH, DATA_DIRS[sf], table + ".tbl")
            command = (
                "BULK INSERT "
                + table
                + " FROM '"
                + data_path
                + "' WITH ( FORMAT = 'CSV', FIELDTERMINATOR='|');"
            )
            execute_sqlserver_sql(command, DATABASES[sf])
    return 

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="setup_tpch")
    parser.add_argument("-sf", "--scalefactor")
    parser.add_argument("-t", "--table")
    parser.add_argument("-f", "--flavor")
    args = parser.parse_args()
    tables: List[str] = [args.table] if args.table else tpch_tables
    scalefactor: List[float] = (
        [float(args.scalefactor)] if args.scalefactor else tpch_scalefactors
    )
    print(args)
    if args.flavor == "sqlserver":
        setup_sqlserver(scalefactor, tables)
    else: 
        setup(scalefactor, tables)