#!/usr/bin/env python
"""
Interact with an Oracle or postgres database from the shell.

This program makes a query and presents the result on stdout.  Queries
can be any SQL statement.  The tool uses the db abstractions present in
the desdm despydb pckage. These abstrations mitigate some selected
differences in the SQL dialects spoken by Oracle and Postgres.

The package uses the .desdmservices file  to locate the database
service and to obtain credentials for authentication.

Single queries can be passed as an argument on the command line.

However if the argument is - (dash) standard, queries are read from
standard input.  Lines beginning with #, and lines that are all
whitespace are ignored; Other lines are assumed to contain a query and
are passed to the data base engine. Newlines, not semicolins delimit
queries.

Additionally, if the argument is +, standard input is read, and all lines
are sent to ORACLE as one query. This allows for readable multi line queries.
(A nice extention would be to allow for multiple ; seperated queries.

Queries with errors cause the program to terminate with a non zero exit code.

Any output is printed on stdout in pretty-printed columns.  However, the pretty
printed format not suitable for queries  returning a very large nmber of rows,
as all results from the query are buffered in memory prior to printing. Queries returning
a large number of rows should be printed with csv format, which streams to standard out.

Logging -- The -d (debug) switch logs two lines for every query to stderr. the first line
is logged before the submission of the query, It contains the time the query was submitted
along with  the query itself.  After the query completes, the time to complete the query is
logged to stderr.

Additionally, queries can be logged to a file.  The log file is  specified by the
--log option or, if --log is absent, by the environment variable DESPYDB_QUERY_LOG


Example:

query.py "select count(*) from LOCATION where run = '20121203'"

query.py  - << EOF
insert statement....
insert statement....
EOF

"""

## FM import cx_Oracle
import sys
import os
import time
import datetime


def query_to_cur(dbh, query, args):
    """return a cursor to a query"""
    if args.debug:
        print(datetime.datetime.strftime(datetime.datetime.now(), "%D %H:%m:%S"), query, file=sys.stderr)
    t0 = time.time()
    cur = dbh.cursor()
    cur.execute(query)
    print("query took", time.time()-t0, "seconds")
    return cur


def stringify(datum, floatfmt="%8.2f"):
    if type(datum) == type(1.0):
        return floatfmt % datum
    else:
        return "{0}".format(datum)


def printPrettyFromCursor(cur, args):
    "print data returned from a query in nice aligned columns"
    rows = []
    #get column headers -- not very ergonomic
    # exit if function does nto return things
    if not cur.description:
        return
    if args.header:
        rows.append([item[0] for item in cur.description])

    rows = rows + cur.fetchall()
    zipped = list(zip(*rows))
    widths = []
    for col in zipped:
        width = max([len(stringify(c)) for c in col])
        widths.append(width)
    fmat = ' '.join(['{:<%d}' % width for width in widths])
    for row in rows:
        row = [stringify(r) for r in row]
        print(fmat.format(*row))


def isNumber(datum):
    try:
        int(datum)
    except ValueError:
        return False
    return True


def printCSVFromCursor(cur, args):
    """ output the query results as a CSV"""
    import csv
    import sys

    if not cur.description:
        return #nothing to print.

    writer = csv.writer(sys.stdout, delimiter=args.delimiter, quotechar='|',
                        lineterminator='\n', quoting=csv.QUOTE_MINIMAL)
    if args.header:
        hdr = [col[0] for col in cur.description]
        writer.writerow(hdr)
    for line in cur:
        writer.writerow(line)


def main():
    import argparse
    import time
    """Create command line arguments"""
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--section', '-s', default='db-desoper',
                        help='section in the .desservices file w/ DB connection information')
    parser.add_argument('--debug', '-d', help='print debug info', default=False, action='store_true')
    parser.add_argument('--header', help='print header derived from query',
                        default=False, action='store_true')
    parser.add_argument('--format', '-f', help='format = csv or pretty',
                        default="pretty", choices=["pretty", "csv"])
    parser.add_argument('--delimiter', help='delimiter for csv', default=",")
    parser.add_argument('--log', help='log queries to file (via append)', default=None)
    parser.add_argument('query', help='query to execute (or -, read from standard in)', default=None)

    args = parser.parse_args()
    if not args.log:
        args.log = os.getenv("DESPYDB_QUERY_LOG") # Undefined -> returns None
    query(args)


def do1Query(dbh, query, args):
    cur = query_to_cur(dbh, query, args)
    if args.log:
        import time
        open(args.log, "a").write("%s: %s\n" % (time.time(), query))
    if args.format == "pretty":
        printPrettyFromCursor(cur, args)
    elif args.format == "csv":
        printCSVFromCursor(cur, args)
    else:
        exit(1)


def query(args):

    import despydb
    ## FM import cx_Oracle
    import os
    dbh = despydb.DesDbi(os.path.join(os.getenv("HOME"), ".desservices.ini"), args.section)
    if args.query not in "-+":
        do1Query(dbh, args.query, args)
    elif args.query == "-":
        line = sys.stdin.readline()
        while line:
            line = line.strip()
            if len(line) == 0 or line[0] == "#":
                pass
            else:
                do1Query(dbh, line, args)
            line = sys.stdin.readline()
    elif args.query == "+":
        lines = sys.stdin.read()
        do1Query(dbh, lines, args)
    dbh.close()


if __name__ == "__main__":
    main()
