#!/usr/bin/env python

# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
    Provide a dialect-neutral interface to DES databases.

    Classes:
        DesDbi - Connects to a Postgresql or Oracle instance of a DES database
                 upon instantiation and the resulting object provides an
                 interface based on the Python DB API with extensions to allow
                 interaction with the database in a dialect-neutral manner.

    Developed at: 
    The National Center for Supercomputing Applications (NCSA).

    Copyright (C) 2011 Board of Trustees of the University of Illinois. 
    All rights reserved.

"""

__version__ = "2.0.0"

import re 
import sys
import copy
from despyserviceaccess import serviceaccess
import time
import socket
from collections import OrderedDict
# importing of DB specific modules done down inside code

import errors
import desdbi_defs as defs

class DesDbi (object):
    """
    Provide a dialect-neutral interface to a DES database.

    During Instantiation of this class, service access parameters are found and
    a connection opened to the database identified.  The resulting object
    exposes several methods from an implementation of a python DB API
    Connection class determined by the service access parameters.  In addition,
    the object provides several methods that allow callers to construct SQL
    statements and to interact with the database without dialect-specific code.

    This class may be used as a context manager whereupon it will automatically
    close the database connection after either commiting the transaction if the
    context is exited without an exception or rolling back the transaction
    otherwise.

    As an example of context manager usage, the following code will open a
    database connection, insert two rows into my_table, print the contents of
    my_table after the insert, commit the insert, and close the connection
    unless some sort of error happens:

        with coreutils.DesDbi () as dbh:
            dbh.insert_many ('my_table', ['col1', 'col2'], [(1,1),(2,2)])
            print dbh.query_simple ('my_table')

    If the insert fails, the transaction will be rolled back and the connection
    closed without an attempt query the table.
    """

    def __init__(self, desfile=None, section=None):
        """
        Create an interface object for a DES database.

        The DES services file and/or section contained therein may be specified
        via the desfile and section arguments.  When omitted default values
        will be used as defined in DESDM-3.  A tag of "db" will be used in all
        cases.

        """

        self.configdict = serviceaccess.parse(desfile,section,'DB')
        self.type       = self.configdict['type']

        serviceaccess.check (self.configdict, 'DB')

        if self.type == 'oracle':
            import oracon
            conClass = oracon.OracleConnection
        elif self.type == 'postgres':
            import pgcon
            conClass = pgcon.PostgresConnection
        else:
            raise errors.UnknownDBTypeError (self.type)

        MAXTRIES = 5
        TRY_DELAY = 10 # seconds
        trycnt = 0
        done = False
        lasterr = ""
        while not done and trycnt < MAXTRIES: 
            trycnt += 1
            try:
                self.con = conClass (self.configdict)
                done = True
            except Exception as e:
                lasterr = str(e).strip()
                timestamp = time.strftime("%x %X", time.localtime())
                print "%s: Error when trying to connect to database: %s" % (timestamp,lasterr)
                if trycnt < MAXTRIES:
                    print "\tRetrying...\n"
                    time.sleep(TRY_DELAY)

        if not done:
            print "Exechost:", socket.gethostname()
            print "Connection information:", str(self)
            #for key in ("user", "type", "port", "server"):
            #    print "\t%s = %s" % (key, self.configdict[key])
            print ""
            raise Exception("Aborting attempt to connect to database.  Last error message: %s" % lasterr)
        elif trycnt > 1: # only print success message if we've printed failure message
            print "Successfully connected to database after retrying."
                

    def __enter__(self):
        "Enable the use of this class as a context manager."

        return self

    def __exit__ (self, exc_type, exc_value, traceback):
        """
        Shutdown the connection to the database when context ends.

        Commit any pending transaction if no exception is raised; otherwise,
        rollback that transaction.  In either case, close the database
        connection.
        """

        if exc_type is None:
            self.commit ()
        else:
            self.rollback ()

        self.close ()

        return False

    def autocommit (self, state = None):
        """
        Return and optionally set autocommit mode.

        If provided state is Boolean, set connection's autocommit mode
        accordingly.

        Return autocommit mode prior to any change.
        """
        a = self.con.autocommit

        if isinstance (state, bool):
            self.con.autocommit = state

        return a

    def close(self):
        """
        Close the current connection, disabling any open cursors.
        """
        return self.con.close()

    def commit(self):
        """
        Commit any pending transaction.
        """
        return self.con.commit()

    def cursor(self, fetchsize = None):
        """
        Return a Cursor object for operating on the connection.

        The not yet implemented fetchsize argument would cause PostgreConnection
        to generate a psycopg2 named cursor configured to fetch the indicated
        number of rows from the database per request needed to fulfill the
        requirements of calls to fetchall(), fetchone(), and fetchmany().  It's
        default behavior is to fetch all rows from the database at once.
        """
        return self.con.cursor(fetchsize)


    def get_column_metadata(self, table_name):
        """
        Return a dictionary of 7-item sequences, with lower case column name keys.
        The sequence values are: 
        (name, type, display_size, internal_size, precision, scale, null_ok) 
        Constants are defined for the sequence indexes in coreutils_defs.py
        """
        cursor = self.cursor()
        sqlstr = 'SELECT * FROM %s WHERE 0=1' % table_name
        if self.type == 'oracle':
            cursor.parse(sqlstr)
        elif self.type == 'postgres':
            cursor.execute(sqlstr)
        else:
            raise errors.UnknownDBTypeError (self.type)
        retval = {} 
        for col in cursor.description:
            retval[col[defs.COL_NAME].lower()] = col
        cursor.close()
        return retval

    def get_column_lengths(self, table_name):
        """
        Return a dictionary of column_name = column_length for the given table
        """
        meta = self.get_column_metadata(table_name)
        res = {}
        for col in meta.values():
            res[col[defs.COL_NAME].lower()] = col[defs.COL_LENGTH]
        return res


    def get_column_names(self, table_name):
        """
        Return a sequence containing the column names of specified table.

        Column names are converted to lowercase.
        """
        meta = self.get_column_metadata(table_name)
        column_names = [d[0].lower() for d in meta.values()]
        return column_names

    def get_column_types (self, table_name):
        """
        Return a dictionary of python types indexed by column name for a table.
        """
        return self.con.get_column_types (table_name)


    def get_named_bind_string (self, name):
        """
        Return a named bind (substitution) string.

        Returns a dialect-specific bind string for use with SQL statement
        arguments specified by name.

        Examples:
            expression:      get_named_bind_string ('abc')
            oracle result:   :abc
            postgres result: %(abc)s
        """
        return self.con.get_named_bind_string (name)

    def get_positional_bind_string (self, pos=1):
        """
        Return a positional bind (substitution) string.

        Returns a dialect-specific bind string for use with SQL statement
        arguments specified by position.

        Examples:
            expression:      get_positional_bind_string ()
            oracle result:   :1
            postgres result: %s
        """
        return self.con.get_positional_bind_string(pos)

    def get_regex_clause (self, target, pattern, case_sensitive = True):
        """
        Return a dialect-specific regular expression matching clause.

        Construct a dialect-specific SQL Boolean expression that matches a
        provided target with a provided regular expression string.  The target
        is assumed to be a column name or bind expression so it is not quoted
        while the pattern is assumed to be a string, so it is quoted.

        Case sensitivity of matching can be controlled.  When case_sensitive is
        None, the Oracle implementation will defer to the database default
        settings.

        For a more flexible interface, refer to get_regex_format().

        Examples:
            expression:      get_regex_clause ("col1", "pre.*suf")
            oracle result:   REGEXP_LIKE (col1, 'pre.*suf')
            postgres result: (col1 ~ 'pre.*suf')

            expression:      get_regex_clause (get_positional_bind_string(),
                                               "prefix.*")
            oracle result:   REGEXP_LIKE (:1, 'prefix.*')
            postgres result: (%s ~ 'prefix.*')
        """
        d = {'target' : target,
             'pattern': "'" + pattern + "'"}

        return self.get_regex_format (case_sensitive) % d

    def get_regex_format (self, case_sensitive = True):
        """
        Return a format string for constructing a regular expression clause.

        The returned format string contains two python named-substitution
        strings:
            target  -- expects string indicating value to compare to
            pattern -- expects string indicating the regular expression
        The value for both should be exactly what is desired in the SQL
        expression which means, for example, that if the regular expression is
        a explicit string rather than a bind string, it should contain the
        single quotes required for strings in SQL.

        When working with constant regular expressions, the get_regex_clause()
        is easier to use.

        Examples:
            expression:      get_regex_format ()
            oracle result:   REGEXP_LIKE (%(target)s, %(pattern)s)
            postgres result: %(target)s ~ %(pattern)s

            expression:      get_regex_format () % {"target": "col1",
                                                    "pattern": "'pre.*suf'"}
            oracle result:   REGEXP_LIKE (col1, 'pre.*suf', 'c')
            postgres result: (col1 ~ 'pre.*suf')

            expression:      get_regex_format () % {
                                    "target":  get_positional_bind_string (),
                                    "pattern": get_positional_bind_string ()}
            oracle result:   REGEXP_LIKE (:1, :1, 'c')
            postgres result: (%s ~ %s)
        """
        return self.con.get_regex_format (case_sensitive)

    def get_seq_next_clause (self, seqname):
        """
        Return an SQL expression that extracts the next value from a sequence.

        Construct and return a dialect-specific SQL expression that, when
        evaluated, will extract the next value from the specified sequence.

        Examples:
            expression:      get_seq_next_clause ('seq1')
            oracle result:   seq1.NEXTVAL
            postgres result: nextval('seq1')
        """
        return self.con.get_seq_next_clause (seqname)

    def get_seq_next_value (self, seqname):
        """
        Return the next value from the specified sequence.

        Execute a dialect-specific expression to extract the next value from
        the specified sequence and return that value.

        Examples:
            expression:           get_seq_next_value ('seq1')
            oracle result from:   SELECT seq1.NEXTVAL FROM DUAL
            postgres result from: SELECT nextval('seq1')
        """
        expr = self.get_seq_next_clause (seqname)
        return self.exec_sql_expression (expr) [0]

    def insert_many (self, table, columns, rows):
        """
        Insert a sequence of rows into the indicated database table.

        Arguments:
            table   Name of the table into which data should be inserted.
            columns Names of the columns to be inserted.
            rows    A sequence of rows to insert.

        If each row in rows is a sequence, the values in each row must be in
        the same order as all other rows and columns must be a sequence
        identifying that order.  If each row is a dictionary or other mapping,
        columns can be any iterable that returns the column names and the set
        of keys for each row must match the set of column names.
        """

        if len (rows) == 0:
            return
        if hasattr (rows [0], 'keys'):
            vals = ','.join ([self.get_named_bind_string (c) for c in columns])
        else:
            bindStr = self.get_positional_bind_string()
            vals = ','.join ([bindStr for c in columns])

        colStr = ','.join (columns)

        stmt = 'INSERT INTO %s (%s) VALUES (%s)' % (table, colStr, vals)

        curs = self.cursor ()
        try:
            curs.executemany (stmt, rows)
        finally:
            curs.close ()

    def insert_many_indiv (self, table, columns, rows):
        """
        Insert a sequence of rows into the indicated database table.

        Arguments:
            table   Name of the table into which data should be inserted.
            columns Names of the columns to be inserted.
            rows    A sequence of rows to insert.

        If each row in rows is a sequence, the values in each row must be in
        the same order as all other rows and columns must be a sequence
        identifying that order.  If each row is a dictionary or other mapping,
        columns can be any iterable that returns the column names and the set
        of keys for each row must match the set of column names.
        """

        if len (rows) == 0:
            return
        if hasattr (rows [0], 'keys'):
            vals = ','.join ([self.get_named_bind_string (c) for c in columns])
        else:
            bindStr = self.get_positional_bind_string()
            vals = ','.join ([bindStr for c in columns])

        colStr = ','.join (columns)

        stmt = 'INSERT INTO %s (%s) VALUES (%s)' % (table, colStr, vals)

        curs = self.cursor ()
        curs.prepare(stmt)
        for row in rows:
            try:
                curs.execute(None, row)
            except Exception as err:
                print "\n\nError: ", err
                print "sql>", stmt
                print "params:", row
                print "\n\n"
                raise
        
        curs.close ()

    def query_simple (self, from_, cols = '*', where = None, orderby = None,
                      params = None, rowtype = dict):
        """
        Issue a simple query and return results.

        Arguments:
            from_       a string containing the name of a table or view or some
                        other from expression
            cols        the columns to retrieve; can be a sequence of column
                        names or expressions or a string containing them; for
                        rowtype = dict, unique aliases should be assigned to
                        expressions so that the keys are reasonable
            where       optional WHERE expression; can be a string containing
                        the where clause minus "WHERE " or a sequence of
                        expressions to be joined by AND.
            orderby     an optional ORDER BY expression; can be a string
                        containing an ORDER BY expression or a sequence of such
                        expressions.
            params      optional bind parameters for the query.
            rowtype     the type of row to return; dict results in a dictionary
                        indexed by the lowercase version of the column names
                        provided by the query results; other types will be
                        passed the retrieved row sequence to be coerced to the
                        desired type

        If positional bind strings are used in column expressions and/or the
        where clause, params should be a sequence of values in the same order.
        If named bind strings are used, params should be a dictionary indexed
        by bind string names.

        The resulting rows are returned as a list of the specified rowtype.

        Example:
            Code:
                dbh = coreutils.DesDbi ()
                cols  = ["col1", "col2"]
                where = ["col1 > 5", "col2 < 'DEF'", "col3 = :1"]
                ord   = cols
                parms = ("col3_value", )
                rows = dbh.query_simple ('tab1', cols, where, ord, parms)
            Possible Output:
               [{"col1": 23, "col2": "ABC"}, {"col1": 45, "col2": "AAA"}]
                
        """

        if not from_:
            raise TypeError ('A table name or other from expression is '
                             'required.')

        if hasattr (cols, '__iter__') and len (cols) > 0:
            colstr = ','.join (cols)
        elif cols:
            colstr = cols
        else:
            raise TypeError ('A non-empty sequence of column names or '
                             'expressions or a string of such is required.')

        if hasattr (where, '__iter__') and len (where) > 0:
            where_str = ' WHERE ' + ' AND '.join (where)
        elif where:
            where_str = ' WHERE ' + where
        else:
            where_str = ''

        if hasattr (orderby, '__iter__') and len (orderby) > 0:
            ord_str = ' ORDER BY ' + ','.join (orderby)
        elif orderby:
            ord_str = ' ORDER BY ' + orderby
        else:
            ord_str = ''

        stmt = "SELECT %s FROM %s%s%s" % (colstr, from_, where_str, ord_str)

        curs = self.cursor ()
        try:
            if params:
                curs.execute (stmt, params)
            else:
                curs.execute (stmt)

            rows = curs.fetchall ()
            rcols = [desc [0].lower () for desc in curs.description]

        finally:
            curs.close ()

        if rowtype == dict:
            res = [{col:val for col, val in zip (rcols, row)} for row in rows]
        elif len (rows) > 0 and type (rows [0]) == rowtype:
            res = rows
        else:
            res = [rowtype (row) for row in rows]

        return res

    def is_postgres(self):
        return self.type == 'postgres'

    def is_oracle(self):
        return self.type == 'oracle'

    def rollback(self):
        """
        Rollback the current transaction.
        """

        return self.con.rollback()

    def sequence_drop (self, seq_name):
        "Drop sequence; do not generate error if it doesn't exist."

        self.con.sequence_drop(seq_name)

    def __str__(self):
        copydict = copy.deepcopy(self.configdict)
        del copydict['passwd']
        return '%s' % (copydict)

    def table_drop (self, table):
        "Drop table; do not generate error if it doesn't exist."

        self.con.table_drop(table)

    def from_dual (self):
        return self.con.from_dual()

    def which_services_file(self):
        return self.configdict['meta_file']

    def which_services_section(self):
        return self.configdict['meta_section']

    def quote(self, value):
        return "'%s'" % str(value).replace("'","''")

    def get_current_timestamp_str(self):
        """
        return string for current timestamp
        """
        return self.con.get_current_timestamp_str()

    def query_results_dict(self, sql, tkey):
        curs = self.cursor()
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]

        result = OrderedDict()
        for line in curs:
            d = dict(zip(desc, line))
            result[d[tkey.lower()].lower()] = d

        curs.close()
        return result



    def basic_insert_row (self, table, row):
        """ Insert a row into the table """

        ctstr = self.get_current_timestamp_str()

        cols = row.keys()
        namedbind = []
        params = {}
        for col in cols:
            if row[col] == ctstr:
                namedbind.append(row[col])
            else:
                namedbind.append(self.get_named_bind_string(col))
                params[col] = row[col]

        sql = "insert into %s (%s) values (%s)" % (table,
                                                   ','.join(cols),
                                                   ','.join(namedbind))


        curs = self.cursor()
        try:
            curs.execute(sql, params)
        except:
            (type, value, traceback) = sys.exc_info()
            print "******************************"
            print "Error:", type, value
            print "sql> %s\n" % (sql)
            print "params> %s\n" % (params)
            raise



    def basic_update_row (self, table, updatevals, wherevals):
        """ Update a row in a table """

        ctstr = self.get_current_timestamp_str()

        params = {}
        whclause = []
        for c,v in wherevals.items():
            if v == ctstr:
                whclause.append("%s=%s" % (c, v))
            else:
                whclause.append("%s=%s" % (c, self.get_named_bind_string('w_'+c)))
                params['w_'+c] = v

        upclause = []
        for c,v in updatevals.items():
            if v == ctstr:
                upclause.append("%s=%s" % (c, v))
            else:
                upclause.append("%s=%s" % (c, self.get_named_bind_string('u_'+c)))
                params['u_'+c] = v


        sql = "update %s set %s where %s" % (table, ','.join(upclause), 
                                             ' and '.join(whclause))

        curs = self.cursor()
        try:
            curs.execute(sql, params)
        except:
            (type, value, traceback) = sys.exc_info()
            print "******************************"
            print "Error:", type, value
            print "sql> %s\n" % (sql)
            print "params> %s\n" % params
            raise

        if curs.rowcount == 0:
            print "******************************"
            print "sql> %s\n" % sql
            print "params> %s\n" % params
            raise Exception("Error: 0 rows updated in table %s" % table)

        curs.close()
   

#### Embedded simple test
if __name__ ==  '__main__' :
    dbh = DesDbi()
    print 'dbh = ', dbh
    if dbh.is_postgres():
        print 'Connected to postgres DB'
    elif dbh.is_oracle():
        print 'Connected to oracle DB'
    print 'which_services_file = ', dbh.which_services_file()
    print 'which_services_section = ', dbh.which_services_section()

    print dbh.get_column_names('exposure')

    cursor = dbh.cursor()
    cursor.execute ('SELECT count(*) from exposure')
    row = cursor.fetchone()
    print 'Number exposures:', row[0]
    cursor.close()
    #dbh.commit()
    dbh.close()
