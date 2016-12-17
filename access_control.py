from functools import wraps

import sqlparse


UNKNOWN = 'unknown'

# Disallow DML statments on source
DISALLOW_SOURCE = ['create', 'alter', 'insert', 'drop', 'truncate', 'update',
                   'delete', 'rename']

# Disallow DML statements on result
DISALLOW_RESULT = ['create', 'alter', 'drop', 'truncate', 'delete',
                   'rename']


def check_permission(func):
    @wraps(func)
    def permission_wrapper(self, *args, **kwargs):
        parsed = sqlparse.parse(kwargs.get('query'))
        command_type = sqlparse.sql.Statement(parsed[0]).get_type().lower()
        if (self.source and command_type in DISALLOW_SOURCE) or \
           (not self.source and command_type in DISALLOW_RESULT) or \
           (command_type == UNKNOWN):
            print 'NOT ALLOWED'
            self.connection.close()
            if kwargs.get('cursor'):
                try:
                    kwargs['cursor'].close()
                except AttributeError:
                    pass
        else:
            return func(self, *args, **kwargs)
    return permission_wrapper

