import MySQLdb as mysqldb


class Connect(object):
    def __init__(self, *args, **kwargs):
        """
        Create DB connection

        host: string, host to connect

        user: string, user to connect as

        passwd: string, password to use

        db: string, database to use

        port: integer, TCP/IP port to connect to

        See MySQLdb for more parameters
        """
        self.connection = mysqldb.Connection(*args, **kwargs)

    def dbquery(self, query, cursor=None, **kwargs):
        if cursor:
            return _query_cursor(cursor, query, **kwargs)
        else:
            return _query_connection(self.connection, query, **kwargs)


def _query_connection(connection, query, **kwargs):
    cursor = connection.cursor()
    try:
        for row in _query_cursor(cursor, query, **kwargs):
            yield row
    except StopIteration:
        cursor.close()


def _query_cursor(cursor, query, **kwargs):
    cursor.execute(query, **kwargs)

    if not cursor.rowcount:
        f_row = None
        yield f_row

    for row in cursor:
        yield row

