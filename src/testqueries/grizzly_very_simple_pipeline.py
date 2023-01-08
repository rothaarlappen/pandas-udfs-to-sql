import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from database import create_connection
connection = create_connection().raw_connection()

import grizzly
from grizzly.relationaldbexecutor import RelationalExecutor
from grizzly.sqlgenerator import SQLGenerator

def comment_len(comment: str) -> int:
    return str(len(comment))

grizzly.use(RelationalExecutor(connection, SQLGenerator("postgresql")))

df = grizzly.read_table("orders")

df["comment_length"] = df["o_comment"].map(comment_len) # apply myfunc

print(df.collect()[:5])