# pandas-udfs-to-sql
This repository provides a translator for pandas udfs into sql udf to enhance enhance performance working with pandas dataframes.

To convert the exemplary scripts in `src/testqueries/` execute:

    - `python3 -m src.converter.convert src/testqueries/<SCRIPT> <PERSIST_MODE>`

where SCRIPT is equal to the to be converted file (e.g. simple_pipeline.py) and PERSIST_MODE is either MATERIALIZED_VIEW or
NEW_TABLE (runtimes don't seem to differ much).

# Benchmarks
The benchmark requires the following environment variables (e.g. exposed in a .env file):  

```
pg_scalefactor="0.01"
db_scalefactor='0.01'

# specifies which server you want to use: either "postgresql" or "sqlserver" 
flavor="sqlserver"

# required for postgres-baseline/grizzly/translated pipelines (materialized view/new table)
pg_pw="<postgres_server_password>" 
pg_host="<postgres_server_ip>" 
pg_user="<postgres_user_name>" 

# required for sqlserver-baseline/sqlserver translated pipelines
sqlserver_pw="<sqlserver_password>"
sqlserver_host="<sqlserver_host>" # "localhost" or ip
sqlserver_user="<sqlserver_user>" # most likely "SA"
```