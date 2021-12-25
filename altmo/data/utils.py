import re as _re
from typing import Callable, Generator

from psycopg2 import extensions as _ext


async def execute_values(cur, sql, argslist, template=None, page_size=100, fetch=False):
    """
    This has been copied from the psycopg2 library.

    Execute a statement using :sql:`VALUES` with a sequence of parameters.

    :param cur: the cursor to use to execute the query.

    :param sql: the query to execute. It must contain a single ``%s``
        placeholder, which will be replaced by a `VALUES list`__.
        Example: ``"INSERT INTO mytable (id, f1, f2) VALUES %s"``.

    :param argslist: sequence of sequences or dictionaries with the arguments
        to send to the query. The type and content must be consistent with
        *template*.

    :param template: the snippet to merge to every item in *argslist* to
        compose the query.

        - If the *argslist* items are sequences it should contain positional
          placeholders (e.g. ``"(%s, %s, %s)"``, or ``"(%s, %s, 42)``" if there
          are constants value...).

        - If the *argslist* items are mappings it should contain named
          placeholders (e.g. ``"(%(id)s, %(f1)s, 42)"``).

        If not specified, assume the arguments are sequence and use a simple
        positional template (i.e.  ``(%s, %s, ...)``), with the number of
        placeholders sniffed by the first element in *argslist*.

    :param page_size: maximum number of *argslist* items to include in every
        statement. If there are more items the function will execute more than
        one statement.

    :param fetch: if `!True` return the query results into a list (like in a
        `~cursor.fetchall()`).  Useful for queries with :sql:`RETURNING`
        clause.

    .. __: https://www.postgresql.org/docs/current/static/queries-values.html

    After the execution of the function the `cursor.rowcount` property will
    **not** contain a total result.

    While :sql:`INSERT` is an obvious candidate for this function it is
    possible to use it with other statements, for example::

        >>> cur.execute(
        ... "create table test (id int primary key, v1 int, v2 int)")

        >>> execute_values(cur,
        ... "INSERT INTO test (id, v1, v2) VALUES %s",
        ... [(1, 2, 3), (4, 5, 6), (7, 8, 9)])

        >>> execute_values(cur,
        ... '''UPDATE test SET v1 = data.v1 FROM (VALUES %s) AS data (id, v1)
        ... WHERE test.id = data.id''',
        ... [(1, 20), (4, 50)])

        >>> cur.execute("select * from test order by id")
        >>> cur.fetchall()
        [(1, 20, 3), (4, 50, 6), (7, 8, 9)])
    """
    from psycopg2.sql import Composable
    if isinstance(sql, Composable):
        sql = sql.as_string(cur)

    # we can't just use sql % vals because vals is bytes: if sql is bytes
    # there will be some decoding error because of stupid codec used, and Py3
    # doesn't implement % on bytes.
    if not isinstance(sql, bytes):
        sql = sql.encode(_ext.encodings[cur.connection.encoding])
    pre, post = _split_sql(sql)

    result = [] if fetch else None
    for page in _paginate(argslist, page_size=page_size):
        if template is None:
            template = b'(' + b','.join([b'%s'] * len(page[0])) + b')'
        parts = pre[:]
        for args in page:
            parts.append(cur.mogrify(template, args))
            parts.append(b',')
        parts[-1:] = post
        await cur.execute(b''.join(parts))
        if fetch:
            result.extend(cur.fetchall())

    return result


def _split_sql(sql):
    """Split *sql* on a single ``%s`` placeholder.

    Split on the %s, perform %% replacement and return pre, post lists of
    snippets.
    """
    curr = pre = []
    post = []
    tokens = _re.split(br'(%.)', sql)
    for token in tokens:
        if len(token) != 2 or token[:1] != b'%':
            curr.append(token)
            continue

        if token[1:] == b's':
            if curr is pre:
                curr = post
            else:
                raise ValueError(
                    "the query contains more than one '%s' placeholder")
        elif token[1:] == b'%':
            curr.append(b'%')
        else:
            raise ValueError("unsupported format character: '%s'"
                             % token[1:].decode('ascii', 'replace'))

    if curr is pre:
        raise ValueError("the query doesn't contain any '%s' placeholder")

    return pre, post


def _paginate(seq, page_size):
    """Consume an iterable and return it in chunks.

    Every chunk is at most `page_size`. Never return an empty chunk.
    """
    page = []
    it = iter(seq)
    while True:
        try:
            for i in range(page_size):
                page.append(next(it))
            yield page
            page = []
        except StopIteration:
            if page:
                yield page
            return


def page_query(func: Callable, count: int, batch_size: int, *args, **kwargs) -> Generator:
    """
    This function is used to iterate over potentially very large query result sets.
    `func` should defined kwargs `start` and `limit` and these kwargs should limit the query result accordingly
    """
    for batch_start in range(0, count, batch_size):
        kwargs |= {'start': batch_start, 'limit': batch_size}
        yield func(*args, **kwargs)
