#!/usr/bin/env python

# $Id: test_desdbi.py 10292 2013-01-07 17:54:32Z mgower $
# $Rev:: 10292                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2013-01-07 11:54:32 #$:  # Date of last commit.

"""
    Test DesDbi class via unittest

    Synopsis:
        test dbtype [unittest_parameters]

    dbtype must be either "oracle" or "postgres".  A DES services file will be
    found using methods defined in DESDM-3.  The file is expected to contain a
    section named according to dbtype:

        oracle      db-oracle-unittest
        postgres    db-postgres-unittest

    The database user thus identified should have permission to create
    sequences and tables within its own schema.

    Any unittest_parameters are passed on to the python unittest module.

    Classes:
        DesDbiTest - Simulates a project-specific DesDbi subclass with its
                     its own database access extensions.  In this case, the
                     extensions are useful for the test cases.

        TestDesDbi - Defines the test cases.

    Developed at: 
    The National Center for Supercomputing Applications (NCSA).
  
    Copyright (C) 2012 Board of Trustees of the University of Illinois. 
    All rights reserved.

"""

__version__ = "$Rev: 10292 $"

import ConfigParser
import datetime
import sys
import unittest
import despydb

_dbType = None

class DesDbiTest (despydb.DesDbi, despydb.DBTestMixin):
    """
    Define a few methods to simulate expected usage of DesDbi.

    The DesDbi class is expected to be subclassed by various projects with
    database access methods necessary for each project defined in that
    project's subclass.  This class simulates that use and provide methods for
    use by the test cases.
    """

    def __init__ (self, *args, **kwargs):
        despydb.DesDbi.__init__ (self, *args, **kwargs)

    def select_all (self, table, columns):
        stmt = 'SELECT %s FROM %s' % (','.join (columns), table)
        curs = self.cursor ()
        try:
            curs.execute (stmt)
            res = curs.fetchall ()
        except Exception:
            self.rollback ()
            raise
        finally:
            curs.close ()

        return res

    def run_autocommit (self, autocommit):
        """
        Test autocommit using the specified mode.

        autocommit parameter may be Boolean to explicitly set the autocommit
        mode for the test or None to implicitly use current mode with the
        expectation that it is off.
        """

        # Create (or recreate) test table.
        table = 'test_autocommit'

        self.table_drop (table)

        self.table_create (table, 'col1 integer')

        # This code is meant to test autocommit mode, so can't assume one
        # way or the other and postgres requires that DDL activity be commited
        # before other connections can set it, so commit the current transaction
        # just in case.

        self.commit ()

        # Get a second connection to make a change independent of the main test
        # connection to determine whether changes made via the second
        # connection can be seen from the main test connection.

        dbh2 = despydb.DesDbi (section=self.which_services_section())

        # Set the second connection's autocommit mode as instructed.

        if autocommit is not None:
            dbh2.autocommit (autocommit)

        try:
            # Insert a row without an explicit commit and try to access it
            # from the main connection.  Record the result to return to
            # caller.

            cursor2 = dbh2.cursor ()

            stmt = ('INSERT INTO %s (col1) VALUES (%s)' %
                    (table, self.get_positional_bind_string ()))
            cursor2.execute (stmt, (4, ))

            try:
                cursor1 = self.cursor ()
                cursor1.execute ('SELECT col1 FROM %s' % table)
                res = cursor1.fetchone ()
            except Exception:
                self.rollback ()
                raise
            finally:
                cursor1.close ()
        except Exception:
            dbh2.rollback ()
            raise
        finally:
            cursor2.close ()
            dbh2.close ()
            self.table_drop (table)
            self.commit ()

        return res

class TestDesDbi (unittest.TestCase):
    """
    Test DesDbi class.

    Provided methods test the ability to create a connection and interact with
    a DES database.
    """

    @classmethod
    def setUpClass (cls):
        # Open a connection for use by all tests.  This opens the possibility of
        # tests interferring with one another, but connecting for each test
        # seems a bit excessive.

        if _dbType == 'oracle':
            cls.testSection = 'db-oracle-unittest'
        elif _dbType == 'postgres':
            cls.testSection = 'db-postgres-unittest'

        try:
            cls.dbh = DesDbiTest (section=cls.testSection)
        except ConfigParser.NoSectionError as exc:
            msg = ('Error: Cannot find the "%s" section in the DES services '
                   'file.\nTo reduce the chances of damange to production '
                   'systems when this script drops\nand creates tables and '
                   'other database objects, database connection information\n'
                   'is taken only from a specific section of the DES services '
                   'file.  Add the\nindicated section to yours to run this '
                   'test.'
                  ) % exc.section
            sys.exit (msg)

    @classmethod
    def tearDownClass (cls):
        cls.dbh.close ()

    def setUp (self):
        pass

    def test_autocommit_default (self):
        "Autocommit mode should be off by default."
        res = self.dbh.run_autocommit (None)
        self.assertIsNone (res)

    def test_autocommit_off (self):
        "Autocommit mode should be off when explicitly disabled."
        res = self.dbh.run_autocommit (False)
        self.assertIsNone (res)

    def test_autocommit_on (self):
        "Autocommit mode should be on when explicitly enabled."
        res = self.dbh.run_autocommit (True)
        self.assertIsNotNone (res)
        self.assertEqual (res [0], 4)

    def test_get_column_names (self):
        "get_column_names() should return the expected column names."

        table   = 'test_table_cols'
        columns = ['col1', 'col2', 'col3']

        with self.dbh.table_recreate (table, columns, 'integer'):
            try:
                res = self.dbh.get_column_names (table)
            except Exception:
                self.dbh.rollback ()
                raise

        self.assertEqual (res, columns)

    def test_get_column_types (self):
        "get_column_types() should return the expected python types."

        table = 'test_table_cols'

        columnPythonTypes = {'col1': float,
                             'col2': datetime.datetime,
                             'col3': str,
                             'col4': str
                            }
        columnDBTypes = {
            'col1': 'integer',
            'col2': 'date'         if _dbType == 'oracle' else 'timestamp',
            'col3': 'varchar2 (3)' if _dbType == 'oracle' else 'varchar (3)',
            'col4': 'char (1)'
            }

        columnDefs = ','.join ([c + ' ' + v for c, v in columnDBTypes.items()])
        with self.dbh.table_recreate (table, columnDefs):
            try:
                res = self.dbh.get_column_types (table)
            except Exception:
                self.dbh.rollback ()
                raise

        self.assertEqual (res, columnPythonTypes)

    def test_insert_many_dict (self):
        "insert_many() should insert rows given as a list of dictionaries."

        table   = 'test_insert'
        columns = ['col1', 'col2', 'col3']

        in_vals  = [{'col1' : 1, 'col2' : 2, 'col3': 3},
                    {'col1' : 4, 'col2' : 5, 'col3': 6},
                    {'col1' : 7, 'col2' : 8, 'col3': 9}
                   ]
        out_vals = [tuple ([row [col] for col in columns]) for row in in_vals]

        with self.dbh.table_recreate (table, columns, 'integer'):
            try:
                self.dbh.insert_many (table, columns, in_vals)
                res = self.dbh.select_all (table, columns)
            except Exception:
                self.dbh.rollback ()
                raise

        self.assertEqual (res, out_vals)

    def test_insert_many_list (self):
        "insert_many() should insert rows given as a list of lists."

        table   = 'test_insert'
        columns = ['col1', 'col2', 'col3']

        out_values = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]
        in_values  = [list (tup) for tup in out_values]

        with self.dbh.table_recreate (table, columns, 'integer'):
            try:
                self.dbh.insert_many (table, columns, in_values)
                res = self.dbh.select_all (table, columns)
            except Exception:
                self.dbh.rollback ()
                raise

        self.assertEqual (res, out_values)

    def test_insert_many_tuple (self):
        "insert_many() should insert rows given as a list of tuples."

        table   = 'test_insert'
        columns = ['col1', 'col2', 'col3']
        values  = [(1, 2, 3), (4, 5, 6), (7, 8, 9)]

        with self.dbh.table_recreate (table, columns, 'integer'):
            try:
                self.dbh.insert_many (table, columns, values)
                res = self.dbh.select_all (table, columns)
            except Exception:
                self.dbh.rollback ()
                raise

        self.assertEqual (res, values)

    def test_named_bind_string (self):
        "Returned named bind strings should work with a dictionary."

        binds = ['v1', 'v2', 'v3']

        qry  = self.dbh.get_expr_exec_format() % ','.join (
                [self.dbh.get_named_bind_string (v) for v in binds])

        cursor = self.dbh.cursor ()
        try:
            cursor.execute (qry, dict ([(v, 3) for v in binds]))
            self.assertEqual (cursor.fetchone (), tuple ([3 for v in binds]))
        finally:
            cursor.close ()
            self.dbh.rollback ()

    def test_positional_bind_string (self):
        "Returned positional bind strings should work with a sequence."

        vals = (3, 2, 1)
        qry  = self.dbh.get_expr_exec_format() % ','.join (
                [self.dbh.get_positional_bind_string () for v in vals])

        cursor = self.dbh.cursor ()
        try:
            cursor.execute (qry, vals)
            self.assertEqual (cursor.fetchone (), vals)
        finally:
            cursor.close ()
            self.dbh.rollback ()

    def test_query_simple (self):
        "Simple query with defaults."

        tab = 'query_test'
        cols = ['col1', 'col2']
        rows_in = [{'col1':r[0],'col2':r[1]} for r in [(1, 2), (2, 4), (3, 6)]]

        with self.dbh.table_recreate (tab, cols, 'integer'):
            self.dbh.insert_many (tab, cols, rows_in)
            rows_out = self.dbh.query_simple (tab, cols)
            self.assertEqual (rows_in, rows_out)
    
    def test_query_simple_colstr (self):
        "Simple query using a column string and aliases."

        tab = 'query_test'
        cols = ['col1', 'col2']
        rows_in = [[1, 2], [2, 4], [3, 6]]
        colstr = 'col1 + col2 as colsum, col1 * col2 as colprod'
        rows_expected = [{'colsum': 3, 'colprod': 2},
                         {'colsum': 6, 'colprod': 8},
                         {'colsum': 9, 'colprod': 18}]

        with self.dbh.table_recreate (tab, cols, 'integer'):
            self.dbh.insert_many (tab, cols, rows_in)
            rows_out = self.dbh.query_simple (tab, colstr, orderby = cols)
            self.assertEqual (rows_expected, rows_out)
    
    def test_query_simple_custom_rowtype (self):
        "Simple query using a custom rowtype."

        tab = 'query_test'
        cols = ['col1', 'col2']
        rows_in = [(1, 2), (2, 4), (3, 6)]
        rows_expected =  [{row [0], row [1]} for row in rows_in]

        with self.dbh.table_recreate (tab, cols, 'integer'):
            self.dbh.insert_many (tab, cols, rows_in)
            rows_out = self.dbh.query_simple (tab, cols, orderby = cols,
                                              rowtype = set)
            self.assertEqual (rows_expected, rows_out)
    
    def test_query_simple_param_dict (self):
        "Simple query using named bind string."

        tab = 'query_test'
        cols = ['col1', 'col2']
        colstr = self.dbh.get_named_bind_string ('tst1')
        rows_in = [(1, 2), (2, 4), (3, 6)]
        rows_expected = [(9, ), (9, ), (9, )]
        params = {'tst1' : 9}

        with self.dbh.table_recreate (tab, cols, 'integer'):
            self.dbh.insert_many (tab, cols, rows_in)
            rows_out = self.dbh.query_simple (tab, colstr, params = params,
                                              rowtype = tuple)
            self.assertEqual (rows_expected, rows_out)
    
    def test_query_simple_param_seq (self):
        "Simple query using positional bind string."

        tab = 'query_test'
        cols = ['col1', 'col2']
        colstr = self.dbh.get_positional_bind_string ()
        rows_in = [(1, 2), (2, 4), (3, 6)]
        rows_expected = [(9, ), (9, ), (9, )]
        params = (9, )

        with self.dbh.table_recreate (tab, cols, 'integer'):
            self.dbh.insert_many (tab, cols, rows_in)
            rows_out = self.dbh.query_simple (tab, colstr, params = params,
                                              rowtype = tuple)
            self.assertEqual (rows_expected, rows_out)
    
    def test_query_simple_tuple (self):
        "Simple query retrieving tuples."

        tab = 'query_test'
        cols = ['col1', 'col2']
        rows_in = [(1, 2), (2, 4), (3, 6)]

        with self.dbh.table_recreate (tab, cols, 'integer'):
            self.dbh.insert_many (tab, cols, rows_in)
            rows_out = self.dbh.query_simple (tab, cols, rowtype = tuple)
            self.assertEqual (set (rows_in), set (rows_out))
    
    def test_query_simple_list (self):
        "Simple query retrieving tuples."

        tab = 'query_test'
        cols = ['col1', 'col2']
        rows_in = [[1, 2], [2, 4], [3, 6]]

        with self.dbh.table_recreate (tab, cols, 'integer'):
            self.dbh.insert_many (tab, cols, rows_in)
            rows_out = self.dbh.query_simple (tab, cols, orderby = cols,
                                              rowtype = list)
            self.assertEqual (rows_in, rows_out)
    
    def test_query_simple_where_seq (self):
        "Simple query using a sequence of where expressions."

        tab = 'query_test'
        cols = ['col1', 'col2']
        rows_in = [[1, 2], [2, 4], [2, 4], [3, 6]]
        rows_expected =  [(2,4), (2,4)]
        where = ['col1 = 2', 'col2 = 4']

        with self.dbh.table_recreate (tab, cols, 'integer'):
            self.dbh.insert_many (tab, cols, rows_in)
            rows_out = self.dbh.query_simple (tab, cols, where = where,
                                              rowtype = tuple)
            self.assertEqual (rows_expected, rows_out)
    
    def test_query_simple_where_str (self):
        "Simple query using a column string and aliases."

        tab = 'query_test'
        cols = ['col1', 'col2']
        rows_in = [[1, 2], [2, 4], [3, 6]]
        rows_expected =  [(1, 2), (2,4)]
        where = '2 in (col1, col2)'

        with self.dbh.table_recreate (tab, cols, 'integer'):
            self.dbh.insert_many (tab, cols, rows_in)
            rows_out = self.dbh.query_simple (tab, cols, where = where,
                                              rowtype = tuple)
            self.assertEqual (rows_expected, rows_out)
    
    def test_regex_bad_case_sensitivity (self):
        "An invalid case sensitivity flag should be reported."

        with self.assertRaises (despydb.UnknownCaseSensitiveError):
            self.dbh.get_regex_clause ("'ABC'", 'a.*', 'F')

    def test_regex_case_insensitive_match (self):
        "Case-insensitive regular expressions should work."

        cursor = self.dbh.cursor ()
        try:
            expr = self.dbh.get_regex_clause ("'ABC'", 'a.*', False)
            qry  = self.dbh.get_expr_exec_format() % "'TRUE'"
            qry += ' WHERE ' + expr

            cursor.execute (qry)

            self.assertEqual (cursor.fetchone ()[0], 'TRUE')
        finally:
            self.dbh.rollback ()
            cursor.close ()

    def test_regex_case_sensitive_match (self):
        "Case-sensitive regular expressions should work."

        cursor = self.dbh.cursor ()
        try:
            expr = self.dbh.get_regex_clause ("'abc'", 'a.*')
            qry  = self.dbh.get_expr_exec_format() % "'TRUE'"
            qry += ' WHERE ' + expr

            cursor.execute (qry)

            self.assertEqual (cursor.fetchone ()[0], 'TRUE')
        finally:
            self.dbh.rollback ()
            cursor.close ()

    def test_regex_case_sensitive_nomatch (self):
        "Case-sensitive regular expressions should not match the wrong case."

        cursor = self.dbh.cursor ()
        try:
            expr = self.dbh.get_regex_clause ("'ABC'", 'a.*')
            qry  = self.dbh.get_expr_exec_format() % "'TRUE'"
            qry += ' WHERE ' + expr

            cursor.execute (qry)

            self.assertIsNone (cursor.fetchone ())
        finally:
            self.dbh.rollback ()
            cursor.close ()

    def test_sequence (self):
        "Retrieving values from a sequence should work."

        seq_name = 'test_seq'

        with self.dbh.sequence_recreate (seq_name):
            try:
                self.assertEqual (self.dbh.get_seq_next_value (seq_name), 1)
                self.assertEqual (self.dbh.get_seq_next_value (seq_name), 2)
                self.assertEqual (self.dbh.get_seq_next_value (seq_name), 3)
            except Exception:
                self.dbh.rollback ()
                raise

    def test_table_drop_exist (self):
        "Dropping an existing table should drop the table."

        table = "test_drop"

        if not self.dbh.table_can_query (table):
            self.dbh.table_create (table, 'c1 integer')

        self.dbh.table_drop (table)

        self.assertFalse (self.dbh.table_can_query (table))

    def test_table_drop_notexist (self):
        "Should be able to drop a table that doesn't exist without error."

        table = "test_drop"

        # Drop twice in case the table currently exists.
        self.dbh.table_drop (table)
        self.dbh.table_drop (table)

if __name__ == '__main__':
    if sys.hexversion < 0x02070000:
        sys.exit (sys.argv [0] + ': Error: Python version >= 2.7 and < 3.0 '
                  'required.') 

    usage = 'Usage: %s.py oracle|postgres [unittest_args...]' % sys.argv [0]

    try:
        _dbType = sys.argv [1]
    except IndexError:
        sys.exit (usage) 

    if _dbType not in ['oracle', 'postgres']:
        sys.exit (usage) 

    del sys.argv [1]

    unittest.main ()
