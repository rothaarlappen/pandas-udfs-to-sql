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
)
from typing import List

PSQL_PATH = "C:\\Program Files\\PostgreSQL\\13\\bin\\psql.exe"
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


if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog="setup_tpch")
    parser.add_argument("-sf", "--scalefactor")
    parser.add_argument("-t", "--table")

    args = parser.parse_args()
    tables: List[str] = [args.table] if args.table else tpch_tables
    scalefactor: List[float] = (
        [float(args.scalefactor)] if args.scalefactor else tpch_scalefactors
    )

    setup(scalefactor, tables)
