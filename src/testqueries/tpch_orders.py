import pandas as pd
import sys
from os import path
sys.path.append(path.dirname( path.dirname( path.abspath(__file__))))
from database import create_connection

df = pd.read_sql("SELECT * FROM udfs.orders_tpch", create_connection())
print(df.head())


