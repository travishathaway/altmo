import contextlib
from functools import wraps

import aiopg
import psycopg2
from psycopg2.extras import NamedTupleCursor, register_hstore as psycopg2_register_hstore

from altmo.settings import get_config


@get_config
def psycopg2_cur(config):
    """Wrap function to setup and tear down a Postgres connection while
    providing a cursor object to make queries with.
    """

    def wrap(f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            # Setup postgres connection
            connection = psycopg2.connect(config.PG_DSN)

            try:
                cursor = connection.cursor(cursor_factory=NamedTupleCursor)
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
def psycopg_context(conn_info):
    # Setup postgres connection
    connection = psycopg2.connect(conn_info)

    try:
        cursor = connection.cursor()
        yield cursor
    finally:
        # Close connection
        connection.commit()
        connection.close()


def async_postgres_pool(func):
    @wraps(func)
    @get_config
    async def wrapper(config, *args, **kwargs):
        async with aiopg.create_pool(config.PG_DSN, timeout=600) as pool:
            return await func(pool, *args, **kwargs)
    return wrapper


def async_postgres_cursor(func):
    @wraps(func)
    @async_postgres_pool
    async def wrapper(pool, *args, **kwargs):
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                return await func(cursor, *args, **kwargs)
    return wrapper


def async_postgres_cursor_method(func):
    @wraps(func)
    @async_postgres_pool
    async def wrapper(pool, *args, **kwargs):
        async with pool.acquire() as conn:
            async with conn.cursor() as cursor:
                self, *_args = args
                return await func(self, cursor, *_args, **kwargs)
    return wrapper


def register_hstore(func):
    """
    Intended to decorate funcs which take the psycopg2 cursor object as a first argument.
    """
    @wraps(func)
    def wrapper(cursor, *args, **kwargs):
        psycopg2_register_hstore(cursor)
        return func(cursor, *args, **kwargs)
    return wrapper
