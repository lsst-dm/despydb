"""
    Define psycopg2-specific database access methods

    Classes:
        PostgresConnection - Connects to a PostgreSQL instance of a DES
                             database upon instantiation and the resulting
                             object provides an interface based on the pscyopg2
                             connection class with extensions to allow
                             interaction with the database in a dialect-neutral
                             manner.

    Developed at: 
    The National Center for Supercomputing Applications (NCSA).
  
    Copyright (C) 2012 Board of Trustees of the University of Illinois. 
    All rights reserved.

"""

__version__ = "$Rev$"

import datetime
import random

import psycopg2
import psycopg2.errorcodes
from psycopg2.extensions import connection as pgConnection

import errors

# The first time get_column_types() is used, it will populate this mapping
# from psycopg2 type_code values to python types.
_TYPE_MAP = None


def _make_type_map():
    "Populate the global _TYPE_MAP."

    global _TYPE_MAP
    _TYPE_MAP = {}
    for code in psycopg2.extensions.string_types:
        # Maybe there's an easier way. :-(
        if code == psycopg2.BINARY:
            ptype = bytearray
        elif code == psycopg2.DATETIME:
            ptype = datetime.datetime
        elif code == psycopg2.NUMBER:
            ptype = float
        elif code == psycopg2.ROWID:
            ptype = bytearray
        elif code == psycopg2.STRING:
            ptype = str
        elif code == psycopg2.extensions.BOOLEAN:
            ptype = bool
        elif code == psycopg2.extensions.DATE:
            ptype = datetime.date
        elif code == psycopg2.extensions.FLOAT:
            ptype = float
        elif code == psycopg2.extensions.INTERVAL:
            ptype = datetime.timedelta
        elif code == psycopg2.extensions.TIME:
            ptype = datetime.time
        elif code == psycopg2.extensions.UNICODE:
            ptype = unicode
        else: # Ignore other types for now.
            ptype = None
        if ptype:
            _TYPE_MAP[code] = ptype


class PostgresConnection (pgConnection):
    """
    Provide psycopg2-specific implementations of canonical database methods

    Refer to desdbi.py for full method documentation.
    """

    def __init__(self, access_data):
        """
        Initialize a PostgresConnection object

        Connect the PostgresConnection instance to the database identified in
        access_data.

        """

        dsn = 'host=%s dbname=%s user=%s password=%s port=%s' % (
              access_data['server'], access_data['name'],
              access_data['user'], access_data['passwd'],
              access_data['port'])

        pgConnection.__init__(self, dsn)

    def cursor(self, fetchsize=None):
        "Return a psycopg2 Cursor object for operating on the connection."

        if fetchsize:
            # Tell psycopg2 to create a server-side cursor so that it will
            # return the results in batches of the indicated size instead of
            # retrieving all results on the first fetch.

            #name = some unique, valid cursor name
            #curs = pgConnection.cursor (self, name)
            #curs.itersize = fetchsize
            raise NotImplementedError('Need algorithm for unique cursor name.')
        else:
            curs = pgConnection.cursor(self)

        return curs

    def get_column_types(self, table_name):
        """
        Return a dictionary of python types indexed by column name for a table.
        """

        if _TYPE_MAP is None:
            # On the first call, build a map from some of the type codes to a
            # python type.
            _make_type_map()

        cursor = self.cursor()
        cursor.execute('SELECT * FROM %s WHERE 0=1' % table_name)

        types = {d[0].lower(): _TYPE_MAP[d[1]] for d in cursor.description}

        cursor.close()

        return types

    def get_expr_exec_format(self):
        "Return a format string for a statement to execute SQL expressions."

        return 'SELECT %s'

    def get_named_bind_string(self, name):
        "Return a named bind (substitution) string for name with psycopg2."

        return "%%(%s)s" % name

    def get_positional_bind_string(self, pos=1):
        "Return a positional bind (substitution) string for psycopg2."

        return "%s"

    def get_regex_format(self, case_sensitive=True):
        """
        Return a format string for constructing a regular expression clause.

        See DesDbi class for detailed documentation.
        """

        # Use POSIX regular expressions
        if case_sensitive is True:
            oper = '~'
        elif case_sensitive is False:
            oper = '~*'
        elif case_sensitive is None:
            oper = '~' # postgres doesn't have a global config option
        else:
            raise errors.UnknownCaseSensitiveError(value=case_sensitive)

        return "(%%(target)s %s %%(pattern)s)" % oper

    def get_seq_next_clause(self, seqname):
        "Return an SQL expression that extracts the next value from a sequence."

        return "nextval('" + seqname + "')"

    def sequence_drop(self, seq_name):
        "Drop sequence; do not generate error if it doesn't exist."

        curs = self.cursor()

        svp = '"svp_drop_seq_%s"' % random.randint(0, 9999999)
        curs.execute('SAVEPOINT ' + svp)

        stmt = 'DROP SEQUENCE IF EXISTS %s' % seq_name

        try:
            curs.execute(stmt)
        except (psycopg2.ProgrammingError, psycopg2.InternalError) as exc:
            curs.execute('ROLLBACK TO SAVEPOINT ' + svp)
            if exc.pgcode != psycopg2.errorcodes.INSUFFICIENT_PRIVILEGE:
                raise
        finally:
            curs.execute('RELEASE SAVEPOINT ' + svp)
            curs.close()

    def table_drop(self, table):
        "Drop table; do not generate error if it doesn't exist."

        curs = self.cursor()

        # The main reason this function exists is to allow tests to drop tables
        # used for testing before creating a fresh table in a known state
        # without all the code for checking whether the table already exists.
        # Some of those test tables are copies in the test user's schema of
        # public tables.  In this case if the table doesn't exist in the
        # current schema, Postgres will attempt to drop the public table and
        # throw an exception since the test user doesn't own the public table.
        # Create a savepoint (with a hopefully unique name) and rollback to
        # that if any errors occur, but catch the privilege exception.

        svp = '"svp_drop_table_%s"' % random.randint(0, 9999999)
        curs.execute('SAVEPOINT ' + svp)

        stmt = 'DROP TABLE IF EXISTS %s' % table

        try:
            curs.execute(stmt)
        except (psycopg2.ProgrammingError, psycopg2.InternalError) as exc:
            curs.execute('ROLLBACK TO SAVEPOINT ' + svp)
            if exc.pgcode != psycopg2.errorcodes.INSUFFICIENT_PRIVILEGE:
                raise
        finally:
            curs.execute('RELEASE SAVEPOINT ' + svp)
            curs.close()

    def from_dual(self):
        """ return empty string as PostgreSQL doesn't use 'from dual' """
        return ""

    def get_current_timestamp_str(self):
        return "now()"
