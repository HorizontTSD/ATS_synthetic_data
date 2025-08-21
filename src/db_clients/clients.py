import os
import sqlite3
import psycopg2
from dotenv import load_dotenv

load_dotenv()

home_path = os.getcwd()

test_db_path = os.path.join(home_path, "src", "db_sandbox", "test_db.sqlite")

def test_get_db_connection(db_path=None):
    if db_path is None:
        db_path = os.getenv("SQLITE_DB_PATH", test_db_path)
    return sqlite3.connect(db_path)


dbname = os.getenv("PG_DB")
user = os.getenv("PG_USER")
password = os.getenv("PG_PASSWORD")
host = os.getenv("PG_HOST")
port = os.getenv("PG_PORT")

def get_db_connection():
    return psycopg2.connect(
        dbname=dbname,
        user=user,
        password=password,
        host=host,
        port=port,
    )
