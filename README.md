# pandas-udfs-to-sql
This repository provides a translator for pandas udfs into sql udf to enhance enhance performance working with pandas dataframes.

To convert the exemplary scripts in `src/testqueries/` execute:

    - `python3 -m src.converter.convert src/testqueries/<SCRIPT> <PERSIST_MODE>`

where SCRIPT is equal to the to be converted file (e.g. simple_pipeline.py) and PERSIST_MODE is either MATERIALIZED_VIEW or
NEW_TABLE (runtimes don't seem to differ much).