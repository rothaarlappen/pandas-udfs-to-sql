import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from database import create_connection


conn = create_connection()

conn.execute("DROP MATERIALIZED VIEW IF EXISTS table_new")
conn.execute(
    "CREATE MATERIALIZED VIEW table_new AS SELECT *, length(o_comment) AS comment_length, date_part('YEAR', o_orderdate) AS order_year, left(o_orderpriority, 1)::integer AS order_priority_numeric FROM orders"
)
