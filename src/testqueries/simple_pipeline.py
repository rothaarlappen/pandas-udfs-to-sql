import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from database import create_connection
import pandas as pd
from pandas import Timestamp

conn = create_connection()


def comment_len(comment: str) -> int:
    return str(len(comment))


def order_year(orderdate: Timestamp) -> int:
    return str(orderdate)[:4]


def order_priority_numeric(orderpriority: str) -> int:
    return int(orderpriority[0])


df = pd.read_sql("SELECT * FROM orders", conn)

df["comment_length"] = df.apply(lambda row: comment_len(row["o_comment"]), axis=1)
df["order_year"] = df.apply(lambda row: order_year(row["o_orderdate"]), axis=1)
df["order_priority_numeric"] = df.apply(
    lambda row: order_priority_numeric(row["o_orderpriority"]), axis=1
)

df.to_sql("table_new", conn)
