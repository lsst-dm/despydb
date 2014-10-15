# $Id: Makefile 12357 2013-06-27 02:03:47Z donaldp $
# $Rev:: 12357                            $:  # Revision of last commit.
# $LastChangedBy:: donaldp                $:  # Author of last commit.
# $LastChangedDate:: 2013-06-26 21:03:47 #$:  # Date of last commit.

# Override any environment variable called "prefix".  This would normally be
# set to /usr/local, but that's not reasonable for this package.
prefix=

unittestargs=--quiet

# The oraTest and pgTest variables are evaluated later to define prerequisites
# for the check target.  They use serviceAccess to search the environment for
# the unit test sections and generate the name of the appropriate make target
# for the sections found.

saCommand:=serviceAccess
oraSec=db-desoper
pgSec=db-postgres-unittest
oraTest=$(shell $(saCommand) -s $(oraSec) "test_oracle" 2>/dev/null)
pgTest=$(shell $(saCommand) -s $(pgSec) "test_postgres" 2>/dev/null)
oraMsg=CoreUtils: Warning: Missing $(oraSec) section; Oracle tests skipped.
pgMsg=CoreUtils: Warning: Missing $(pgSec) section; PostgreSQL tests skipped.
errMsg=CoreUtils: Error: No database tests could be performed.

SHELL=/bin/sh
CFLAGS=-g -Wall
# Need the include path even if CFLAGS is overridden by invoker
CFLAGS += -I../include
COREUTILSLIB=../lib/coreutilslib.a
export PYTHONPATH:=../python:$(PYTHONPATH)
export PATH:=../bin:$(PATH)

.PHONY: all check clean install test_serviceaccess uninstall
.PHONY: test_desdbi test_desdbi_oracle test_desdbi_postgres
.PHONY: test_dbtestmixin test_dbtestmixin_oracle test_dbtestmixin_postgres

all: test_svc_parse

install:
	@true

uninstall:
	@true

check: test_serviceaccess $(oraTest) $(pgTest)
	@[ -n "$(oraTest)" ] || echo "$(oraMsg)"
	@[ -n "$(pgTest)" ] || echo "$(pgMsg)"
	@if [ -z "$(oraTest)$(pgTest)" ]; then echo "$(errMsg)"; false; fi

test_desdbi: test_desdbi_oracle test_desdbi_postgres  test_query.py

test_desdbi_oracle:
	./test_desdbi.py oracle $(unittestargs)

test_desdbi_postgres:
	./test_desdbi.py postgres $(unittestargs)

test_dbtestmixin: test_dbtestmixin_oracle test_dbtestmixin_postgres

test_dbtestmixin_oracle:
	./test_dbtestmixin.py oracle $(unittestargs)

test_dbtestmixin_postgres:
	./test_dbtestmixin.py postgres $(unittestargs)

test_oracle: test_desdbi_oracle test_dbtestmixin_oracle

test_postgres: test_desdbi_postgres test_dbtestmixin_postgres

test_query.py: 
	cat   query.py.sql | query.py --format=csv  -             >/dev/null
	cat   query.py.sql | query.py --format=pretty -           >/dev/null
	cat   query.py.sql | query.py --header --format=pretty -  >/dev/null
	cat   query.py.sql | query.py --header --format=pretty -  >/dev/null
	query.py "select * from LOCATION where ROWNUM < 10"       >/dev/null 

test_serviceaccess: test_svc_parse
	./test_serviceaccess.py $(unittestargs)

test_svc_parse: $(COREUTILSLIB)(svc_parse.o ini.o) test_svc_parse.c
	gcc $(CFLAGS) -o test_svc_parse test_svc_parse.c $(COREUTILSLIB)
	: test against the .deservices.ini file in this test directory
	: test that we can fetch from the default file, by using HOME to find the .desservices file
	if [ `(HOME=./ ; ./test_svc_parse  passwd db-desoper)` != fake_password ] ;  then echo FAIL ; exit 1 ; fi
	: test that we can fetch from an explicity passed in services file
	if [ `(./test_svc_parse  passwd db-desoper ./.desservices.ini)` != fake_password ] ;  then echo FAIL ; exit 1  ; fi
	: test that we can fetch from a  services file found in the environment
	if [ `(export DES_SERVICES=./.desservices.ini ; ./test_svc_parse  passwd db-desoper)` != fake_password ] ;  then echo FAIL ; exit 1  ; fi
	: test that we can fetch from a section specified in the environment
	if [ `(export DES_DB_SECTION=db-desoper ; ./test_svc_parse  passwd "" ./.desservices.ini)` != fake_password ] ;  then echo FAIL ; exit 1  ; fi
	: test that the package can sense the absense of the config files -- implying user is not converted
	(HOME=/tmp ; env -u DES_DB_SECTION -u DES_SERVICES ./test_svc_parse  -s -v  passwd /tmp/db-desoper 2>&1 | grep --quiet "can't sense the presence")


clean: 
	rm -f  *~ \#*\#
	rm -rf *.pyc
	rm -f test_svc_parse 
