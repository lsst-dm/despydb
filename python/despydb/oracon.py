"""Define cx_Oracle-specific database access methods

Classes:
    OracleConnection - Connects to an Oracle instance of a DES database
                       upon instantiation and the resulting object provides
                       an interface based on the cx_Oracle Connection class
                       with extensions to allow callers to interact with
                       the database in a dialect-neutral manner.

Developed at:
The National Center for Supercomputing Applications (NCSA).

Copyright (C) 2012 Board of Trustees of the University of Illinois.
All rights reserved.
"""

import datetime
import socket
import warnings

import cx_Oracle

from . import errors

# Construct a name for the v$session module column to allow database auditing.

import __main__
try:
    _MODULE_NAME = __main__.__file__
except AttributeError:
    _MODULE_NAME = "unavailable"

if _MODULE_NAME.rfind('/') > -1:
    _MODULE_NAME = _MODULE_NAME[_MODULE_NAME.rfind('/') + 1:]
_MODULE_NAME += '@' + socket.getfqdn()
_MODULE_NAME = _MODULE_NAME[:48]

# No need to rebuild this mapping every time it is used, so make it a global
# module object.
_TYPE_MAP = {cx_Oracle.BINARY: bytearray,
             cx_Oracle.BFILE: cx_Oracle.BFILE,
             cx_Oracle.BLOB: bytearray,
             cx_Oracle.CLOB: str,
             cx_Oracle.CURSOR: cx_Oracle.CURSOR,
             cx_Oracle.DATETIME: datetime.datetime,
             cx_Oracle.FIXED_CHAR: str,
             cx_Oracle.FIXED_UNICODE: str,
             cx_Oracle.INTERVAL: datetime.timedelta,
             cx_Oracle.LOB: bytearray,
             cx_Oracle.LONG_BINARY: bytearray,
             cx_Oracle.LONG_STRING: str,
             cx_Oracle.NATIVE_FLOAT: float,
             cx_Oracle.NCLOB: str,
             cx_Oracle.NUMBER: float,
             cx_Oracle.OBJECT: cx_Oracle.OBJECT,
             cx_Oracle.ROWID: bytearray,
             cx_Oracle.STRING: str,
             cx_Oracle.TIMESTAMP: datetime.datetime,
             cx_Oracle.UNICODE: str
             }

# Define some symbolic names for oracle error codes to make it clearer what
# the error codes mean.

_ORA_NO_TABLE_VIEW = 942    # table or view does not exist
_ORA_NO_SEQUENCE = 2289   # sequence does not exist


class OracleConnection (cx_Oracle.Connection):
    """Provide cx_Oracle-specific implementations of canonical database methods

    Refer to desdbi.py for full method documentation.
    """

    def __init__(self, access_data):
        """Initialize an OracleConnection object.

        Connect the OracleConnection instance to the database identified in
        access_data.
        """
        user = access_data['user']
        pswd = access_data['passwd']

        kwargs = {'host': access_data['server'], 'port': access_data['port']}

        # Take SID first as specified by DESDM-3.
        if access_data.get('sid', None):
            kwargs['sid'] = access_data['sid']
        elif access_data.get('name', None):
            kwargs['service_name'] = access_data['name']
        else:
            raise errors.MissingDBId()

        if cx_Oracle.version > '5.1':
            dsn = cx_Oracle.makedsn(**kwargs)
        else:
            # Previous makedsn() versions do not support service name
            if 'sid' in kwargs:
                cdt = "(SID=%s)" % kwargs['sid']
            else:
                cdt = "(SERVICE_NAME=%s)" % kwargs['service_name']
            dsn = ("(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=%s)(PORT=%s))"
                   "(CONNECT_DATA=%s))") % (kwargs['host'], kwargs['port'], cdt)

        try:
            cx_Oracle.Connection.__init__(self, user=user, password=pswd,
                                          dsn=dsn, module=_MODULE_NAME)
        except TypeError as exc:
            if str(exc.message).startswith("'module' is an invalid keyword"):
                warnings.warn('Cannot set module name; cx_Oracle upgrade '
                              'recommended.')
                cx_Oracle.Connection.__init__(self, user=user, password=pswd,
                                              dsn=dsn)
            else:
                raise

    def cursor(self, fetchsize=None):
        """Return a cx_Oracle Cursor object for operating on the connection.

        The fetchsize argument is ignored, but retained for compatibility
        with other connection types.
        """
        # cx_Oracle doesn't implement/need named cursors, so ignore fetchsize.

        return cx_Oracle.Connection.cursor(self)

    def get_column_types(self, table_name):
        """Return a dictionary of python types indexed by column name for a table.
        """
        curs = self.cursor()
        curs.execute('SELECT * FROM %s WHERE 0=1' % table_name)

        types = {d[0].lower(): _TYPE_MAP[d[1]] for d in curs.description}

        curs.close()

        return types

    def get_expr_exec_format(self):
        """Return a format string for a statement to execute SQL expressions.
        """
        return 'SELECT %s FROM DUAL'

    def get_named_bind_string(self, name):
        """Return a named bind (substitution) string for name with cx_Oracle.
        """
        return ":" + name

    def get_positional_bind_string(self, pos=1):
        """Return a positional bind (substitution) string for cx_Oracle.
        """
        return ":%d" % pos

    def get_regex_format(self, case_sensitive=True):
        """Return a format string for constructing a regular expression clause.

        See DesDbi class for detailed documentation.
        """
        if case_sensitive is True:
            param = ", 'c'"
        elif case_sensitive is False:
            param = ", 'i'"
        elif case_sensitive is None:
            param = '' # Leave it up to the database to decide
        else:
            raise errors.UnknownCaseSensitiveError(value=case_sensitive)

        return "REGEXP_LIKE (%%(target)s, %%(pattern)s%s)" % param

    def get_seq_next_clause(self, seqname):
        """Return an SQL expression that extracts the next value from a sequence.
        """
        return seqname + '.NEXTVAL'

    def sequence_drop(self, seq_name):
        """Drop sequence; do not generate error if it doesn't exist.
        """
        stmt = 'DROP SEQUENCE %s' % seq_name

        curs = self.cursor()
        try:
            curs.execute(stmt)
        except cx_Oracle.DatabaseError as exc:
            if exc.args[0].code != _ORA_NO_SEQUENCE:
                raise
        finally:
            curs.close()

    def table_drop(self, table):
        """Drop table; do not generate error if it doesn't exist.
        """
        stmt = 'DROP TABLE %s' % table

        curs = self.cursor()
        try:
            curs.execute(stmt)
        except cx_Oracle.DatabaseError as exc:
            if exc.args[0].code != _ORA_NO_TABLE_VIEW:
                raise
        finally:
            curs.close()

    def from_dual(self):
        return "from dual"

    def get_current_timestamp_str(self):
        return "SYSTIMESTAMP"
