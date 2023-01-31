import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from database import create_connection


conn = create_connection()
conn.execute("SELECT * FROM orders")
