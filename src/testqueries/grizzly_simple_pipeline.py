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


grizzly.use(RelationalExecutor(connection, SQLGenerator("postgresql")))

df = grizzly.read_table("orders")

df["comment_length"] = df["o_comment"].map(comment_len)
df["order_year"] = df["o_orderdate"].map(order_year)
df["order_priority_numeric"] = df["o_orderpriority"].map(order_priority_numeric)

print(df.collect()[:5])
