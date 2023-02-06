import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from database import create_connection
import pandas as pd
from pandas import Timestamp

conn = create_connection()

sql = """
EXECUTE sp_execute_external_script @language = N'Python'
    , @script = N'
from pandas import Timestamp

df =InputDataSet

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

OutputDataSet = df'
    , @input_data_1 = N'SELECT * FROM orders;'
WITH RESULT SETS(
	([o_orderkey] [int] NOT NULL,
	[o_custkey] [int] NOT NULL,
	[o_orderstatus] [char](1) NOT NULL,
	[o_totalprice] float NOT NULL,
	[o_orderdate] [date] NOT NULL,
	[o_orderpriority] [char](15) NOT NULL,
	[o_clerk] [char](15) NOT NULL,
	[o_shippriority] [int] NOT NULL,
	[o_comment] [varchar](79) NOT NULL,
	comment_length int not null,
    order_year int not null,
    order_priority_numeric int not null,
    comment_length_1 int not null,
    order_year_1 int not null,
    order_priority_numeric_1 int not null,
    comment_length_2 int not null,
    order_year_2 int not null,
    order_priority_numeric_2 int not null,
    comment_length_3 int not null
));
"""

df = pd.read_sql(sql, conn)
df.head()