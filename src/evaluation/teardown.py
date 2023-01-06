from database import create_connection

conn = create_connection()


def teardown():
    try:
        conn.execute("DROP TABLE IF EXISTS table_new")
    except:
        pass
    try:
        conn.execute("DROP MATERIALIZED VIEW IF EXISTS table_new")
    except:
        pass
