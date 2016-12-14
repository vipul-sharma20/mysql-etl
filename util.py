from functools import wraps

import sqlparse


def _prepare_colnames(columns):
    return ','.join(columns)


def _prepare_placeholder(columns):
    return ','.join(['%s'] * len(columns))


# Decorators Below

def check_connection(func):
    @wraps(func)
    def connection_wrapper(self, query, **kwargs):
        if not self.connection:
            # :TODO: Log statement here
            print 'Connection lost'
            return
        # :TODO: Log statement here
        func(self, **kwargs)
    return connection_wrapper


def prepare_query(skeleton):
    def build(func):
        @wraps(func)
        def query_wrapper(self, table, tablename, query, **kwargs):
            parsed = sqlparse.parse(skeleton)
            command_type = sqlparse.sql.Statement(parsed[0]).get_type().lower()
            if command_type == 'insert':
                columns = table.next()
                column_str = _prepare_colnames(columns)
                placeholder = _prepare_placeholder(columns)
                insert_query = skeleton.format(tablename,
                                               column_str, placeholder)
                func(self, table, tablename, insert_query,
                     **kwargs)
        return query_wrapper
    return build
