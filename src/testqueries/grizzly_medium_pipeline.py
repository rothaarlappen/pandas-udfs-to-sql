import sys
from os import path
from pandas import Timestamp

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from database import create_connection

connection = create_connection().raw_connection()

import grizzly
from grizzly.relationaldbexecutor import RelationalExecutor
from grizzly.sqlgenerator import SQLGenerator


def comment_len(comment: str) -> int:
    return len(comment)


def order_year(orderdate: Timestamp) -> int:
    return int(str(orderdate)[:4])


def order_priority_numeric(orderpriority: str) -> int:
    return int(orderpriority[0])


def comment_len_1(comment: str) -> int:
    return len(comment)


def order_year_1(orderdate: Timestamp) -> int:
    return int(str(orderdate)[:4])


def order_priority_numeric_1(orderpriority: str) -> int:
    return int(orderpriority[0])


def comment_len_2(comment: str) -> int:
    return len(comment)


def order_year_2(orderdate: Timestamp) -> int:
    return int(str(orderdate)[:4])


def order_priority_numeric_2(orderpriority: str) -> int:
    return int(orderpriority[0])


def comment_len_3(comment: str) -> int:
    return len(comment)


grizzly.use(RelationalExecutor(connection, SQLGenerator("postgresql")))

df = grizzly.read_table("orders")

df["comment_length"] = df["o_comment"].map(comment_len)
df["order_year"] = df["o_orderdate"].map(order_year)
df["order_priority_numeric"] = df["o_orderpriority"].map(order_priority_numeric)

df["comment_length_1"] = df["o_comment"].map(comment_len_1)
df["order_year_1"] = df["o_orderdate"].map(order_year_1)
df["order_priority_numeric_1"] = df["o_orderpriority"].map(order_priority_numeric_1)

df["comment_length_2"] = df["o_comment"].map(comment_len_2)
df["order_year_2"] = df["o_orderdate"].map(order_year_2)
df["order_priority_numeric_2"] = df["o_orderpriority"].map(order_priority_numeric_2)

df["comment_length_3"] = df["o_comment"].map(comment_len_3)

print(df.collect()[:5])
