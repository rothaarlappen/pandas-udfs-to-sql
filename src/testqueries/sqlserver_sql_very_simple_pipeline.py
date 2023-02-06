import sys
import pandas as pd 
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from database import create_connection


conn = create_connection().connect()

conn.execute("DROP TABLE IF EXISTS table_new")

trans = conn.begin()
res = conn.execute("SELECT *, len(o_comment) as comment_len INTO table_new FROM orders")
trans.commit()


