import sys
import pandas as pd 
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from database import create_connection


conn = create_connection().connect()

conn.execute("DROP TABLE IF EXISTS table_new")

trans = conn.begin()
res = conn.execute("""SELECT *, 
len(o_comment) as comment_len,
datepart(YEAR, o_orderdate) AS order_year, 
CAST(left(o_orderpriority, 1) AS integer) as order_priority_numeric
INTO table_new 
FROM orders
""")
trans.commit()


