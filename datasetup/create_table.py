import pandas as pd
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()

def connection_string():
    pg_pw = os.getenv('pg_pw')
    pg_db = os.getenv('pg_db')
    pg_host = os.getenv('pg_host')
    pg_user = os.getenv("pg_user")
    return 'postgresql+psycopg2://{0}:{1}@{2}/{3}'.format(pg_user, pg_pw, pg_host, pg_db)

schema_name = "udfs"

conn = create_engine(connection_string())
conn.execute("CREATE SCHEMA IF NOT EXISTS " + schema_name)

df = pd.read_csv(os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "tpch_100mb_orders.tbl"), delimiter="|")
df.to_sql("orders_tpch", conn, schema_name,if_exists="replace", index=False, chunksize=1000,method="multi")