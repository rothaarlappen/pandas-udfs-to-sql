import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

DATABASES = {
    0.01: "tpch_sf_001",
    0.1: "tpch_sf_01",
    1: "tpch_sf_1",
    5: "tpch_sf_5",
    10: "tpch_sf_10",
}

MASTER_DATABASES = {
    "postgresql" : "idp",
    "sqlserver" : "master"
}

load_dotenv()
PG_PW = os.getenv("pg_pw")
PG_HOST = os.getenv("pg_host")
PG_USER = os.getenv("pg_user")

SQLSERVER_PW = os.getenv("sqlserver_pw")
SQLSERVER_HOST = os.getenv("sqlserver_host")
SQLSERVER_USER = os.getenv("sqlserver_user")

# TODO: change to db_scalefactor
DB_SCALEFACTOR = float(os.getenv("pg_scalefactor") or 0)
SQL_FLAVOR = os.getenv("flavor")

def connectionstring():
    db = MASTER_DATABASES[SQL_FLAVOR] if DB_SCALEFACTOR == 0 else DATABASES[DB_SCALEFACTOR]
    if SQL_FLAVOR == "sqlserver":
        return f"mssql+pyodbc://{SQLSERVER_USER}:{SQLSERVER_PW}@{SQLSERVER_HOST}/{db}?driver=SQL+Server+Native+Client+11.0"
    else: 
        return f"postgresql://{PG_USER}:{PG_PW}@{PG_HOST}/{db}"

def create_connection():
    print(connectionstring())
    return create_engine(connectionstring())    

TPCH_CREATE_TABLE_COMMAND = """
   create table nation  ( n_nationkey  integer not null,
                                n_name       char(25) not null,        
                                n_regionkey  integer not null,
                                n_comment    varchar(152));

    create table region  ( r_regionkey  integer not null,
                                r_name       char(25) not null,        
                                r_comment    varchar(152));

    create table part  ( p_partkey     integer not null,
                            p_name        varchar(55) not null,        
                            p_mfgr        char(25) not null,
                            p_brand       char(10) not null,
                            p_type        varchar(25) not null,        
                            p_size        integer not null,
                            p_container   char(10) not null,
                            p_retailprice decimal(15,2) not null,      
                            p_comment     varchar(23) not null );      

    create table supplier ( s_suppkey     integer not null,
                                s_name        char(25) not null,       
                                s_address     varchar(40) not null,    
                                s_nationkey   integer not null,        
                                s_phone       char(15) not null,       
                                s_acctbal     decimal(15,2) not null,  
                                s_comment     varchar(101) not null);  

    create table partsupp ( ps_partkey     integer not null,
                                ps_suppkey     integer not null,       
                                ps_availqty    integer not null,       
                                ps_supplycost  decimal(15,2)  not null,
                                ps_comment     varchar(199) not null );

    create table customer ( c_custkey     integer not null,
                                c_name        varchar(25) not null,
                                c_address     varchar(40) not null,
                                c_nationkey   integer not null,
                                c_phone       char(15) not null,
                                c_acctbal     decimal(15,2)   not null,
                                c_mktsegment  char(10) not null,
                                c_comment     varchar(117) not null);

    create table orders  ( o_orderkey       integer not null,
                            o_custkey        integer not null,
                            o_orderstatus    char(1) not null,
                            o_totalprice     decimal(15,2) not null,
                            o_orderdate      date not null,
                            o_orderpriority  char(15) not null,
                            o_clerk          char(15) not null,
                            o_shippriority   integer not null,
                            o_comment        varchar(79) not null);

    create table lineitem ( l_orderkey    integer not null,
                                l_partkey     integer not null,
                                l_suppkey     integer not null,
                                l_linenumber  integer not null,
                                l_quantity    decimal(15,2) not null,
                                l_extendedprice  decimal(15,2) not null,
                                l_discount    decimal(15,2) not null,
                                l_tax         decimal(15,2) not null,
                                l_returnflag  char(1) not null,
                                l_linestatus  char(1) not null,
                                l_shipdate    date not null,
                                l_commitdate  date not null,
                                l_receiptdate date not null,
                                l_shipinstruct char(25) not null,
                                l_shipmode     char(10) not null,
                                l_comment      varchar(44) not null);
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
