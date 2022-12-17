import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

DATABASES = {
    0: "tpch_sf_001",  # first db....
    0.01: "tpch_sf_001",
    0.1: "tpch_sf_01",
    1: "tpch_sf_1",
}

load_dotenv()
PG_PW = os.getenv("pg_pw")
PG_HOST = os.getenv("pg_host")
PG_USER = os.getenv("pg_user")
PG_SCALEFACTOR = float(os.getenv("pg_scalefactor") or 0)


def psycopg2_connection_string(scalefactor: int = 0):
    if scalefactor not in DATABASES.keys():
        raise ValueError("This TPC-H scalefactor is not supported")
    pg_db = DATABASES[PG_SCALEFACTOR]
    return f"postgresql+psycopg2://{PG_USER}:{PG_PW}@{PG_HOST}/{pg_db}"


def psql_connectionstring(scalefactor: int = 0):
    if scalefactor not in DATABASES.keys():
        raise ValueError("This TPC-H scalefactor is not supported")
    pg_db = DATABASES[scalefactor]
    return f"postgresql://{PG_USER}:{PG_PW}@{PG_HOST}/{pg_db}"


def create_connection():
    return create_engine(psycopg2_connection_string())


TPCH_CREATE_TABLE_COMMAND = """
    CREATE TABLE NATION  ( N_NATIONKEY  INTEGER NOT NULL,
                                N_NAME       CHAR(25) NOT NULL,
                                N_REGIONKEY  INTEGER NOT NULL,
                                N_COMMENT    VARCHAR(152));

    CREATE TABLE REGION  ( R_REGIONKEY  INTEGER NOT NULL,
                                R_NAME       CHAR(25) NOT NULL,
                                R_COMMENT    VARCHAR(152));

    CREATE TABLE PART  ( P_PARTKEY     INTEGER NOT NULL,
                            P_NAME        VARCHAR(55) NOT NULL,
                            P_MFGR        CHAR(25) NOT NULL,
                            P_BRAND       CHAR(10) NOT NULL,
                            P_TYPE        VARCHAR(25) NOT NULL,
                            P_SIZE        INTEGER NOT NULL,
                            P_CONTAINER   CHAR(10) NOT NULL,
                            P_RETAILPRICE DECIMAL(15,2) NOT NULL,
                            P_COMMENT     VARCHAR(23) NOT NULL );

    CREATE TABLE SUPPLIER ( S_SUPPKEY     INTEGER NOT NULL,
                                S_NAME        CHAR(25) NOT NULL,
                                S_ADDRESS     VARCHAR(40) NOT NULL,
                                S_NATIONKEY   INTEGER NOT NULL,
                                S_PHONE       CHAR(15) NOT NULL,
                                S_ACCTBAL     DECIMAL(15,2) NOT NULL,
                                S_COMMENT     VARCHAR(101) NOT NULL);

    CREATE TABLE PARTSUPP ( PS_PARTKEY     INTEGER NOT NULL,
                                PS_SUPPKEY     INTEGER NOT NULL,
                                PS_AVAILQTY    INTEGER NOT NULL,
                                PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
                                PS_COMMENT     VARCHAR(199) NOT NULL );

    CREATE TABLE CUSTOMER ( C_CUSTKEY     INTEGER NOT NULL,
                                C_NAME        VARCHAR(25) NOT NULL,
                                C_ADDRESS     VARCHAR(40) NOT NULL,
                                C_NATIONKEY   INTEGER NOT NULL,
                                C_PHONE       CHAR(15) NOT NULL,
                                C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
                                C_MKTSEGMENT  CHAR(10) NOT NULL,
                                C_COMMENT     VARCHAR(117) NOT NULL);

    CREATE TABLE ORDERS  ( O_ORDERKEY       INTEGER NOT NULL,
                            O_CUSTKEY        INTEGER NOT NULL,
                            O_ORDERSTATUS    CHAR(1) NOT NULL,
                            O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
                            O_ORDERDATE      DATE NOT NULL,
                            O_ORDERPRIORITY  CHAR(15) NOT NULL,
                            O_CLERK          CHAR(15) NOT NULL,
                            O_SHIPPRIORITY   INTEGER NOT NULL,
                            O_COMMENT        VARCHAR(79) NOT NULL);

    CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,
                                L_PARTKEY     INTEGER NOT NULL,
                                L_SUPPKEY     INTEGER NOT NULL,
                                L_LINENUMBER  INTEGER NOT NULL,
                                L_QUANTITY    DECIMAL(15,2) NOT NULL,
                                L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
                                L_DISCOUNT    DECIMAL(15,2) NOT NULL,
                                L_TAX         DECIMAL(15,2) NOT NULL,
                                L_RETURNFLAG  CHAR(1) NOT NULL,
                                L_LINESTATUS  CHAR(1) NOT NULL,
                                L_SHIPDATE    DATE NOT NULL,
                                L_COMMITDATE  DATE NOT NULL,
                                L_RECEIPTDATE DATE NOT NULL,
                                L_SHIPINSTRUCT CHAR(25) NOT NULL,
                                L_SHIPMODE     CHAR(10) NOT NULL,
                                L_COMMENT      VARCHAR(44) NOT NULL);
"""
TPCH_DROP_TABLE_IF_EXIST_COMMAND = """
    DROP TABLE IF EXISTS orders;
    DROP TABLE IF EXISTS customer;
    DROP TABLE IF EXISTS lineitem;
    DROP TABLE IF EXISTS nation;
    DROP TABLE IF EXISTS partsupp;
    DROP TABLE IF EXISTS part;
    DROP TABLE IF EXISTS region;
    DROP TABLE IF EXISTS supplier;
"""
