import os 
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
def connection_string():
    pg_pw = os.getenv('pg_pw')
    pg_db = os.getenv('pg_db')
    pg_host = os.getenv('pg_host')
    pg_user = os.getenv("pg_user")
    return 'postgresql+psycopg2://{0}:{1}@{2}/{3}'.format(pg_user, pg_pw, pg_host, pg_db)

def create_connection(): 
    return create_engine(connection_string())
