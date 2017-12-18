"""
    Provide DES database access methods

    Classes:
        DesDbi - Connects to a Postgresql or Oracle instance of a DES database
                 upon instantiation and the resulting object provides an
                 interface based on the Python DB API with extensions to allow
                 interaction with the database in a dialect-neutral manner.

    Error Classes:
        MissingDBId (Exception)
        UnknownDBTypeError (NotImplementedError)
        UnknownCaseSensitiveError (NotImplementedError)

    Developed at: 
    The National Center for Supercomputing Applications (NCSA).
  
    Copyright (C) 2012 Board of Trustees of the University of Illinois. 
    All rights reserved.

"""

# Note that pydoc includes documentation for entries in the __all__  list when
# generating documentation for this package.

__all__ = ['DesDbi', 'DBTestMixin', 'MissingDBId', 'ServiceaccessException',
           'UnknownDBTypeError', 'UnknownCaseSensitiveError']

# Make the main class and all the error classes available directly within
# the package to simplify imports for package users.

from .desdbi      import DesDbi
from .dbtestmixin import DBTestMixin
from .errors      import *
