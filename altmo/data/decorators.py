import contextlib
from functools import wraps

import psycopg2

from altmo.settings import get_config


@get_config
def psycopg2_cur(config):
    """Wrap function to set up and tear down a Postgres connection while
    providing a cursor object to make queries with.
    """
    def wrap(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            # Setup postgres connection
            connection = psycopg2.connect(config.PG_DSN)

            try:
                cursor = connection.cursor()
                # Call function passing in cursor
                return_val = f(cursor, *args, **kwargs)
            finally:
                # Close connection
                connection.commit()
                connection.close()

            return return_val

        return wrapper

    return wrap


@contextlib.contextmanager
def psycopg2_context(conn_info):
    """Context manager for PostgreSQL connections"""
    # Setup postgres connection
    connection = psycopg2.connect(conn_info)

    try:
        cursor = connection.cursor()
        yield cursor
    finally:
        # Close connection
        connection.commit()
        connection.close()
