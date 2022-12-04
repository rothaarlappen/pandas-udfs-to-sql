import pandas as pd

from connection import create_connection

df = pd.read_sql("SELECT * FROM udfs.orders_tpch", create_connection())

print(df.head())


