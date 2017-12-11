# $Id$
# $Rev::                                  $:  # Revision of last commit.
# $LastChangedBy::                        $:  # Author of last commit.
# $LastChangedDate::                      $:  # Date of last commit.

"""
    Define exceptions raised by coreutils.

    Classes:
        MissingDBId        - No database identification information found in
                             des services section.  Subclass of Exception.
        UnknownDBTypeError - An unknown database type found in des services
                             section.  Subclass of NotImplementedError.
        UnknownCaseSensitiveError 
                             An unknown case sensitivity option was speciied.
                             Subclass of NotImplementedError.

    Developed at: 
    The National Center for Supercomputing Applications (NCSA).

    Copyright (C) 2011 Board of Trustees of the University of Illinois. 
    All rights reserved.

"""

class MissingDBId (Exception):
    "Service access configuration has missing database identification."

    def __init__ (self, msg = None):
        if not msg:
            msg = 'No database identifier found in service access config.'

        Exception.__init__ (self, msg)

class UnknownDBTypeError (NotImplementedError):
    "Service access configuration identifies an unknown database type."

    def __init__ (self, db_type, msg = None):
        self.db_type = db_type
        if not msg:
            msg = 'database type: "%s"' % self.db_type

        NotImplementedError.__init__ (self, msg)

class UnknownCaseSensitiveError (NotImplementedError):
    "Invalid case sensitivity flag."

    def __init__ (self, value, msg = None):
        self.value = value
        if not msg:
            msg = 'Unknown case sensitivity value: "%s"' % value

        NotImplementedError.__init__ (self, msg)
