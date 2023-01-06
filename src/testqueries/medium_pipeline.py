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


def comment_len_1(comment: str) -> int:
    return str(len(comment))


def order_year_1(orderdate: Timestamp) -> int:
    return str(orderdate)[:4]


def order_priority_numeric_1(orderpriority: str) -> int:
    return int(orderpriority[0])


def comment_len_2(comment: str) -> int:
    return str(len(comment))


def order_year_2(orderdate: Timestamp) -> int:
    return str(orderdate)[:4]


def order_priority_numeric_2(orderpriority: str) -> int:
    return int(orderpriority[0])


def comment_len_3(comment: str) -> int:
    return str(len(comment))


df = pd.read_sql("SELECT * FROM orders", conn)

df["comment_length"] = df.apply(lambda row: comment_len(row["o_comment"]), axis=1)
df["order_year"] = df.apply(lambda row: order_year(row["o_orderdate"]), axis=1)
df["order_priority_numeric"] = df.apply(
    lambda row: order_priority_numeric(row["o_orderpriority"]), axis=1
)
df["comment_length_1"] = df.apply(lambda row: comment_len_1(row["o_comment"]), axis=1)
df["order_year_1"] = df.apply(lambda row: order_year_1(row["o_orderdate"]), axis=1)
df["order_priority_numeric_1"] = df.apply(
    lambda row: order_priority_numeric_1(row["o_orderpriority"]), axis=1
)
df["comment_length_2"] = df.apply(lambda row: comment_len_2(row["o_comment"]), axis=1)
df["order_year_2"] = df.apply(lambda row: order_year_2(row["o_orderdate"]), axis=1)
df["order_priority_numeric_2"] = df.apply(
    lambda row: order_priority_numeric_2(row["o_orderpriority"]), axis=1
)
df["comment_length_3"] = df.apply(lambda row: comment_len_3(row["o_comment"]), axis=1)

df.to_sql("table_new", conn)
