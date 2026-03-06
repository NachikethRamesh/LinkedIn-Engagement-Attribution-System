from contextlib import contextmanager

import psycopg2

from app.config import get_database_url


@contextmanager
def get_connection():
    conn = psycopg2.connect(get_database_url())
    try:
        yield conn
    finally:
        conn.close()