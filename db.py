import MySQLdb as mysqldb
from access_control import check_permission


class Connect(object):
    def __init__(self, source=False, *args, **kwargs):
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
        self.source = source

    @check_permission
    def dbquery(self, query, cursor=None, **kwargs):
        if cursor:
            if isinstance(cursor, mysqldb.cursors.Cursor):
                return self._query_cursor(cursor, query, **kwargs)
            else:
                self.connection.close()
        else:
            return self._query_connection(query, **kwargs)

    def close(self):
        return self.connection.close()

    def _query_connection(self, query, **kwargs):
        cursor = self.connection.cursor()
        try:
            for row in self._query_cursor(cursor, query, **kwargs):
                yield row
        except StopIteration:
            cursor.close()

    def _query_cursor(self, cursor, query, **kwargs):
        try:
            cursor.execute(query, **kwargs)
        except mysqldb.Error:
            self.connection.rollback()
        finally:
            cursor.close()
            self.connection.close()

        if not cursor.rowcount:
            f_row = None
            yield f_row

        for row in cursor:
            yield row

