import distutils
from distutils.core import setup
import os,sys

def testing():
    # Now we perform some testing
    unittestargs="--quiet"

    here = os.getcwd()
    os.chdir("tests")

    # We don't need the postgres test... we don't a db to test
    test_dbi_cmds = ["./test_desdbi.py oracle %s"   % unittestargs,
                     #"./test_desdbi.py postgres %s" % unittestargs,
                     "./test_dbtestmixin.py oracle %s" % unittestargs,
                     #"./test_dbtestmixin.py postgres %s" % unittestargs
                     ]
    test_query_cmds = ["cat   query.py.sql | query.py --format=csv  -             >/dev/null",
                       "cat   query.py.sql | query.py --format=pretty -           >/dev/null",
                       "cat   query.py.sql | query.py --header --format=pretty -  >/dev/null",
                       "cat   query.py.sql | query.py --header --format=pretty -  >/dev/null",
                       "query.py \"select * from LOCATION where ROWNUM < 10\"     >/dev/null"]

    for cmd in test_dbi_cmds +  test_query_cmds:
        print cmd
        os.system(cmd)
    os.chdir(here)
    return

# The main call
setup(name='despydb',
      version ='2.0.0',
      license = "GPL",
      description = "Provide a dialect-neutral interface to DES databases",
      author = "The National Center for Supercomputing Applications (NCSA)",
      packages = ['despydb'],
      package_dir = {'': 'python'},
      data_files=[('ups',['ups/despydb.table'])],
      scripts = ['bin/query.py'],
      )

#testing()


