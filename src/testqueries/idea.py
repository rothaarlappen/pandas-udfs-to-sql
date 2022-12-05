import pandas as pd
import sys
from os import path
sys.path.append(path.dirname( path.dirname( path.abspath(__file__))))
from database import create_connection


# The Idea of our project is, that the following two functions are semantically equivalent 
# Moving the computation/code to the server - like it's done in the second method reduces use
# of client-side resources, hence, improves performance of whole pipeline. 

def original_pandas_code():
    conn = create_connection()

    def commment_len(comment: str) -> int:
        return len(comment)

    df = pd.read_sql("SELECT * FROM udfs.orders_tpch LIMIT 100", conn)

    df["comment_length"] = df.apply(lambda row : commment_len(row["comment"]), axis=1)
    print(df.head())

def move_udf_to_database():
    conn = create_connection()

    conn.execute("""
    CREATE OR REPLACE FUNCTION comment_length(comment text)
    RETURNS int
    AS $$
        return len(comment)
    $$ LANGUAGE plpython3u;
    """
    )
    conn.execute("ALTER TABLE udfs.orders_tpch ADD comment_length int")
    conn.execute("UPDATE udfs.orders_tpch SET comment_length = comment_length(comment)")
    df = pd.read_sql("SELECT * FROM udfs.orders_tpch LIMIT 100", conn)  
    print(df.head())
