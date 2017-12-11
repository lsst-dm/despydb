#!/usr/bin/env python

# $Id: test_dbtestmixin.py 10292 2013-01-07 17:54:32Z mgower $
# $Rev:: 10292                            $:  # Revision of last commit.
# $LastChangedBy:: mgower                 $:  # Author of last commit.
# $LastChangedDate:: 2013-01-07 11:54:32 #$:  # Date of last commit.

"""
    Test DBTestMixin class via unittest

    Synopsis:
        test_dbtestmixin.py dbtype [unittest_parameters]

    dbtype must be either "oracle" or "postgres".  A DES services file will be
    found using methods defined in DESDM-3.  The file is expected to contain a
    section named according to dbtype:

        oracle      db-oracle-unittest
        postgres    db-postgres-unittest

    The database user thus identified should have permission to create
    sequences and tables within its own schema.

    Any unittest_parameters are passed on to the python unittest module.

    Classes:
        DBTestMixinTest - Simulates expected use of the mixin by adding it as
                          a parent of a subclass of DesDbi.

        TestDBTestMixin - Defines the test cases.

    Developed at: 
    The National Center for Supercomputing Applications (NCSA).
  
    Copyright (C) 2012 Board of Trustees of the University of Illinois. 
    All rights reserved.

"""


import ConfigParser
import sys
import unittest
import despydb

class DBTestMixinTest (despydb.DesDbi, despydb.DBTestMixin):
    "Define A subclass of DesDbi and DBTestMixin to be used in tests."

    def __init__ (self, *args, **kwargs):
        despydb.DesDbi.__init__ (self, *args, **kwargs)

class TestDBTestMixin (unittest.TestCase):
    "Test the DBTestMixin class."

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
            cls.dbh = DBTestMixinTest (section=cls.testSection)
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

    def test_sequence_recreate_ctxmgr (self):
        "sequence_recreate() should be usable as a context manager."

        test_seq = 'recreate_test'

        self.dbh.sequence_create (test_seq)

        with self.dbh.sequence_recreate (test_seq):
            self.assertEqual (self.dbh.get_seq_next_value (test_seq), 1)

        self.assertRaises (Exception, self.dbh.get_seq_next_value, test_seq)
        self.dbh.rollback ()

    def test_sequence_recreate_no_ctxmgr (self):
        "sequence_recreate() should be usable as a normal method."

        test_seq = 'recreate_test'

        self.dbh.sequence_recreate (test_seq)

        self.assertEqual (self.dbh.get_seq_next_value (test_seq), 1)

        self.dbh.sequence_drop (test_seq)

    def test_table_create_spec (self):
        test_table = 'create_test'
        self.dbh.table_drop (test_table)

        self.dbh.table_create (test_table, 'col1 integer, col2 integer')

        self.dbh.table_drop (test_table)

    def test_table_create_1col_1type (self):
        test_table = 'create_test'
        self.dbh.table_drop (test_table)

        self.dbh.table_create (test_table, 'col1', 'integer')

        self.dbh.table_drop (test_table)

    def test_table_create_2col_0type (self):
        test_table = 'create_test'
        self.dbh.table_drop (test_table)

        self.dbh.table_create (test_table, ['col1 integer', 'col2 integer'])

        self.dbh.table_drop (test_table)

    def test_table_create_2col_1type (self):
        test_table = 'create_test'
        self.dbh.table_drop (test_table)

        self.dbh.table_create (test_table, ['col1', 'col2'], 'integer')

        self.dbh.table_drop (test_table)

    def test_table_create_2col_2type (self):
        test_table = 'create_test'
        self.dbh.table_drop (test_table)

        self.dbh.table_create (test_table, ['col1', 'col2'],
                                           ['integer', 'integer'])

        self.dbh.table_drop (test_table)

    def test_table_copy_empty (self):
        src_table = 'copy_test_src'
        cpy_table = 'copy_test_copy'

        self.dbh.table_drop (cpy_table)
        self.dbh.table_recreate (src_table, 'col1', 'integer')

        try:
            self.dbh.table_copy_empty (cpy_table, src_table)
        except:
            self.dbh.rollback ()
            raise

        self.assertTrue (self.dbh.table_can_query (cpy_table))

        self.dbh.table_drop (src_table)
        self.dbh.table_drop (cpy_table)

    def test_table_recreate_ctxmgr (self):
        "table_recreate() should be usable as a context manager."

        test_table = 'recreate_test'

        self.dbh.table_create (test_table, 'col1', 'integer')

        with self.dbh.table_recreate (test_table, 'col1', 'integer'):
            self.assertTrue (self.dbh.table_can_query (test_table))

        self.assertFalse (self.dbh.table_can_query (test_table))

    def test_table_recreate_no_ctxmgr (self):
        "table_recreate() should be usable as a normal method."

        test_table = 'recreate_test'

        self.dbh.table_recreate (test_table, 'col1', 'integer')

        self.assertTrue (self.dbh.table_can_query (test_table))

        self.dbh.table_drop (test_table)

if __name__ == '__main__':
    if sys.hexversion < 0x02070000 or sys.hexversion >= 0x03000000:
        sys.exit (sys.argv [0] + ': Error: Python version >= 2.7 and < 3.0 '
                  'required.') 

    usage = '%s: Usage: %s oracle|postgres [unittest_args...]' % (
                                                    sys.argv [0], sys.argv [0])

    try:
        _dbType = sys.argv [1]
    except IndexError:
        sys.exit (usage) 

    if _dbType not in ['oracle', 'postgres']:
        sys.exit (usage) 

    del sys.argv [1]

    unittest.main ()
