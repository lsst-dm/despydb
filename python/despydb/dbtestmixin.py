"""Provide a mixin class for DesDbi sub-classes meant for testing.

Classes:
    DBTestMixin - Adds methods to a DesDbi sub-class that are useful for
                  database access test suites.

Since this is a mixin class, it isn't useful to instantiate it alone or
create subclasses that do not have other parent classes.

Developed at:
The National Center for Supercomputing Applications (NCSA).

Copyright (C) 2011 Board of Trustees of the University of Illinois.
All rights reserved.
"""

__version__ = "$Rev$"

import random


class DBTestMixin (object):
    """A mixin class to add testing functionality to a DesDbi sub-class.

    This class defines a number of methods that typically cannot stand on their
    own.  Instead they add functionality to a subclass of coreutils.DesDbi.

    The added methods are useful for test suites for database access methods.
    """

    def __init__(self):
        # Do not invoke parent class's constructor since this class is meant
        # to be used as a parent class of some other subclass of DesDbi and it
        # should invoke (possibly through other parents) its constructor.
        pass

    def sequence_create(self, seq_name):
        """Create the specified sequence with default initial configuration.
        """
        curs = self.cursor()
        try:
            curs.execute('CREATE SEQUENCE %s' % seq_name)
        finally:
            curs.close()

    def sequence_recreate(self, seq_name):
        """Drop and create the specified sequence with default configuration.

        This method returns a context manager, so it may be used in a with
        statement.
        """
        self.sequence_drop(seq_name)
        self.sequence_create(seq_name)

        class Ctxtmgr:
            """A simple context manager the drops a sequence when complete.
            """

            def __init__(self, con, seq):
                self.con = con
                self.seq = seq

            def __enter__(self):
                pass

            def __exit__(self, typ, val, traceback):
                self.con.sequence_drop(self.seq)

        return Ctxtmgr(self, seq_name)

    def table_copy_empty(self, copy_table, src_table):
        """Create an empty copy of the source table.

        The copy must not already exist.

        With the current implementation, the copy will not have any
        constraints, triggers, indexes, etc. except for NOT NULL constraints.
        This means that some operations could succeed on the copy, but fail on
        the source table.
        """
        stmt = 'CREATE TABLE %s AS SELECT * FROM %s WHERE 0 = 1' % (
            copy_table, src_table)
        cursor = self.cursor()
        cursor.execute(stmt)
        cursor.close()

    def table_can_query(self, table):
        """Return Boolean indicating whether table can be queried.

        Attempt to retrieve zero rows from the indicated object and return the
        results.  If an exception is raised, leave the connection in a usable
        state.

        This method can be used to determine whether some table-like object
        with the specified name exists and the current user has permissions to
        select from it; however, it does not ensure that the object is a table
        or that the object exists in any particular schema.  The provided table
        name can include a schema prefix to test for the latter case.
        """
        curs = self.cursor()

        svp = '"svp_table_query_%s"' % random.randint(0, 9999999)
        curs.execute('SAVEPOINT ' + svp)
        try:
            curs.execute('SELECT 1 FROM %s WHERE 0 = 1' % table)
            ret = True
        except Exception:
            curs.execute('ROLLBACK TO SAVEPOINT ' + svp)
            ret = False
        finally:
            curs.close()

        return ret

    def table_create(self, table, columns, types=None):
        """Create a simple table.

        Create the specified table.  The columns and types argument specify the 
        table definition in ways that depend on their type, providing a number
        of convenient invocation options to minimize clutter in a test suite.

        columns   types   intrepretation
        -------  -------- ----------------------------------------------------
        string   none     columns contains the entire table definition
        string   string   columns names a single column and types provides its
                          type
        sequence None     each entry in columns provides the full definition
                          for a column or other table attribute
        sequence string   each entry in columns names a column and types
                          provides a single type for all columns.
        sequence sequence each entry in columns names a column and the
                          corresponding entry in types provides its type.
        """
        if types is None:
            if hasattr(columns, '__iter__'):
                spec = ','.join(columns)
            else:
                spec = columns
        elif hasattr(types, '__iter__'):
            spec = ','.join(['%s %s' % col for col in zip(columns, types)])
        elif hasattr(columns, '__iter__'):
            spec = ','.join(['%s %s' % (col, types) for col in columns])
        else:
            spec = columns + ' ' + types

        cursor = self.cursor()
        try:
            cursor.execute('CREATE TABLE %s (%s)' % (table, spec))
        finally:
            cursor.close()

    def table_prep_test_copy(self, test_table, src_table, cols, rows):
        """Prepare a copy of an existing table for testing purposes.

        Drop test_table if it exists.  Create a copy of src_table.  Insert rows
        into cols of test_table.  The cols and rows arguments correspond to
        those arguments of DesDbi.insert_many().

        Note that test_table and src_table can be the same if the database is
        configured to automatically access that table in some other schema if
        it doesn't exist in the current user's schema and is configured to
        create new tables in the current user's schema.
        """
        self.table_drop(test_table)
        self.table_copy_empty(test_table, src_table)
        self.insert_many(test_table, cols, rows)

    def table_recreate(self, table, columns, types=None):
        """Drop (if necessary) and create the indicated table.

        The columns and types arguments are the same as for table_create().

        This method also returns a context manager, so it may be used in a with
        statement.
        """
        self.table_drop(table)
        self.table_create(table, columns, types)

        class Ctxtmgr:
            """A simple context manager the drops a table when complete.
            """

            def __init__(self, con, tab):
                self.con = con
                self.tab = tab

            def __enter__(self):
                pass

            def __exit__(self, typ, val, traceback):
                self.con.table_drop(self.tab)

        return Ctxtmgr(self, table)
