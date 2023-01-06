import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from database import create_connection
import pandas as pd
from pandas import Timestamp

conn = create_connection()


def comment_len(comment: str) -> int:
    return str(len(comment))


df = pd.read_sql("SELECT * FROM orders", conn)

df["comment_length"] = df.apply(lambda row: comment_len(row["o_comment"]), axis=1)

df.to_sql("orders_new", conn)
