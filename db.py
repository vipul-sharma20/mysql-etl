import MySQLdb as mysqldb

from access_control import check_permission
from util import _prepare_colnames, prepare_query, check_connection

SQL_INSERT = 'INSERT INTO {0} ({1}) VALUES ({2})'


class Connect(object):
    def __init__(self, source=True, *args, **kwargs):
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
    def dbquery(self, query=None, cursor=None, **kwargs):
        if cursor:
            if isinstance(cursor, mysqldb.cursors.Cursor):
                return self._query_cursor(cursor, query, **kwargs)
            else:
                self.connection.close()
        else:
            return self._query_connection(query, **kwargs)

    def close(self):
        return self.connection.close()

    @check_connection
    def _query_connection(self, query=None, **kwargs):
        cursor = self.connection.cursor()
        try:
            for row in self._query_cursor(cursor, query, **kwargs):
                yield row
        except StopIteration:
            # :TODO: Log statement here
            cursor.close()

    def _query_cursor(self, cursor=None, query=None, **kwargs):
        try:
            cursor.execute(query, **kwargs)
        except mysqldb.Error:
            # :TODO: Log statement here
            print 'Error'
            self.connection.rollback()
        finally:
            cursor.close()
            self.connection.close()

        columns = [name[0] for name in cursor.description]
        yield columns

        if not cursor.rowcount:
            f_row = None
            yield f_row

        for row in cursor:
            yield row

    @check_connection
    @prepare_query(SQL_INSERT)
    @check_permission
    def todb(self, table=None, tablename=None, **kwargs):
        query = kwargs.get('query')
        if query:
            cursor = self.connection.cursor()
            cursor.executemany(query, table)
            cursor.close()
            self.connection.commit()
        else:
            # :TODO: Log statement here
            pass

