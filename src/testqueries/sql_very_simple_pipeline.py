import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from database import create_connection


conn = create_connection()

conn.execute("DROP MATERIALIZED VIEW IF EXISTS table_new")
conn.execute(
    "CREATE MATERIALIZED VIEW table_new AS SELECT *, length(o_comment) FROM orders"
)
