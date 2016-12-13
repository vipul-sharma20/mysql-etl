import sqlparse

from functools import wraps


UNKNOWN = 'unknown'

# Disallow DML statments on source
DISALLOW_SOURCE = ['create', 'alter', 'insert', 'drop', 'truncate', 'update',
                   'delete', 'rename']

# Disallow DML statements on result
DISALLOW_RESULT = ['create', 'alter', 'insert', 'drop', 'truncate', 'delete',
                   'rename']


def check_permission(func):
    @wraps(func)
    def wrapper(self, query, **kwargs):
        parsed = sqlparse.parse(query)
        command_type = sqlparse.sql.Statement(parsed[0]).get_type().lower()
        if (self.source and command_type in DISALLOW_SOURCE) or \
           (not self.source and command_type in DISALLOW_RESULT) or \
           (command_type == UNKNOWN):
            self.connection.close()
            if kwargs.get('cursor'):
                try:
                    kwargs['cursor'].close()
                except AttributeError:
                    pass
        else:
            return func(self, query, **kwargs)
    return wrapper

