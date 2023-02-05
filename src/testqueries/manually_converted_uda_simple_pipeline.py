import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from database import create_connection
import pandas as pd
from pandas import Timestamp

conn = create_connection()


df = pd.read_sql("select o_custkey, uda_price(array_agg(o_totalprice)) as c from orders group by o_custkey", conn)

df.head()
