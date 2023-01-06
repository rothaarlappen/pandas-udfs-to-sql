import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))

from database import create_connection


def teardown():
    conn = create_connection()
    try:
        conn.execute("DROP TABLE IF EXISTS table_new")
    except:
        pass
    try:
        conn.execute("DROP MATERIALIZED VIEW IF EXISTS table_new")
    except:
        pass


if __name__ == "__main__":
    teardown()
