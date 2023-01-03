import pandas as pd
import sys
from os import path

sys.path.append(path.dirname(path.dirname(path.abspath(__file__))))
from database import create_connection

# The Idea of our project is, that the following two functions are semantically equivalent
# Moving the computation/code to the server - like it's done in the second method reduces use
# of client-side resources, hence, improves performance of whole pipeline.


def original_pandas_code():
    conn = create_connection()

    def commment_len(comment: str) -> int:
        return len(comment)

    df = pd.read_sql("SELECT * FROM udfs.orders_tpch", conn)

    df["comment_length"] = df.apply(lambda row: commment_len(row["comment"]), axis=1)
    print(df.head())


def move_udf_to_database():
    conn = create_connection()

    conn.execute(
        """
    CREATE OR REPLACE FUNCTION comment_length(comment text)
    RETURNS int
    AS $$
        return len(comment)
    $$ LANGUAGE plpython3u;
    """
    )

    # Persist changes (comes with side-effects... executing script multiple times is tricky...)
    # conn.execute("ALTER TABLE udfs.orders_tpch ADD comment_length int")
    # conn.execute("UPDATE udfs.orders_tpch SET comment_length = comment_length(comment)")

    # Create View:
    # conn.execute("CREATE VIEW views.df_preprocessed AS SELECT *, comment_length(comment_length) as comment_length FROM udfs.orders_tpch LIMIT 100" );
    # df = pd.read_sql("SELECT * FROM df_preprocessed")

    # Probably best approach for interoperability as we don't change data....
    df = pd.read_sql(
        "SELECT *, comment_length(comment) as comment_length FROM udfs.orders_tpch",
        conn,
    )
    print(df.head())


# WHAT DO WE BENCHMARK?

# Overhead of setup, network, etc.

# comparison to baseline (actual python, ...grizzly, aida, ...)
# What:
# - own examples

# How does it scale
# -> Edge cases where it doesn't make any sense

# Runtime (dissection (setup, communication, execution)), memory usage
# context switches penalty (runtime evaluation)

# df_angry_customer = df.filter(onlyOneStar)
# can be translated to
# df_angry_customer = pd.read_sql("SELECT * FROM udfs.orders WHERE onlyOneStar(comment) = 1")

# TODO: look into containerized UDF execution (performance comparison?)


# Use Lucas timing tool to explain original runtime (on unconverted pipeline)

# analyze pd.read times more
