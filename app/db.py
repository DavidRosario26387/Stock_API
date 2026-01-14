import os
from psycopg2.pool import SimpleConnectionPool
from dotenv import load_dotenv

load_dotenv()

DATABASE_URI = os.getenv("DB_URI")

pool = SimpleConnectionPool(
    minconn=1,
    maxconn=3,
    dsn=DATABASE_URI
)

def get_connection():
    return pool.getconn()

def release_connection(conn):
    pool.putconn(conn)
