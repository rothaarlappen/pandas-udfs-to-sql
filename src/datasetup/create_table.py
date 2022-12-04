import pandas as pd
import os
import sys
from os import path
sys.path.append(path.dirname( path.dirname( path.abspath(__file__))))
from database import create_connection

schema_name = "udfs"

conn = create_connection()

conn.execute("CREATE SCHEMA IF NOT EXISTS " + schema_name)

df = pd.read_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "tpch_100mb_orders.tbl"), delimiter="|")
df.to_sql("orders_tpch", conn, schema_name,if_exists="replace", index=False, chunksize=1000,method="multi")