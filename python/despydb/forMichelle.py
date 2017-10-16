DB_COL_FILENAME = "FILENAME"
DB_COL_COMPRESSION = "COMPRESSION"
DB_GTT_FILENAME = "OPM_FILENAME_GTT"

    def exec_sql_expression(self, expression):
        """
        Execute an SQL expression or expressions.

        Construct and execute an SQL statement from a string containing an SQL
        expression or a list of such strings.  Return a sequence containing a
        result for each column.
        """
        if hasattr(expression, '__iter__'):
            s = ','.join(expression)
        else:
            s = expression

        stmt = self.get_expr_exec_format() % s
        cursor = self.cursor()
        cursor.execute(stmt)
        res = cursor.fetchone()
        cursor.close()
        return res

    def get_expr_exec_format(self):
        """
        Return a format string for a statement to execute SQL expressions.

        The returned format string contains a single unnamed python subsitution
        string that expects a string containing the expressions to be executed.
        Once the expressions have been substituted into the string, the
        resulting SQL statement may be executed.

        Examples:
            expression:      con.get_expr_exec_format()
            oracle result:   SELECT %s FROM DUAL
            postgres result: SELECT %s

            expression:      con.get_expr_exec_format() % 'func1(), func2()'
            oracle result:   SELECT func1(), func2() FROM DUAL
            postgres result: SELECT func1(), func2()
        """
        return self.con.get_expr_exec_format()

    def make_where_clause(self, key, value):
        """ return properly formatted string for a where clause """

        if ',' in value:
            value = value.replace(' ', '').split(',')

        condition = ""
        if type(value) is list:  # multiple values
            extra = []
            ins = []
            nots = []
            for v in value:
                if '%' in v:
                    extra.append(self.make_where_condition(key, v))
                elif '!' in v:
                    nots.append(self.make_where_condition(key, v))
                else:
                    ins.append(self.quote(v))

            if len(ins) > 0:
                condition += "%s IN (%s)" % (key, ','.join(ins))
                if len(extra) > 0:
                    condition += ' OR '

            if len(extra) > 0:
                condition += ' OR '.join(extra)

            if ' OR ' in condition:
                condition = '(%s)' % condition

            if len(nots) > 0:
                condition += ' AND '.join(nots)

        elif '*' in value or '^' in value or '$' in value or '[' in value or ']' in value or '&' in value:
            condition = self.get_regexp_clause(key, value)
        elif '%' in value and '!' not in value:
            condition = '%s like %s' % (key, self.quote(value))
            if '\\' in value:
                condition += " ESCAPE '\\'"
        elif '%' in value and '!' in value:
            condition = '%s not like %s' % (key, self.quote(value))
            if '\\' in value:
                condition += " ESCAPE '\\'"
        elif '!' in value:
            if value.lower() == 'null':
                condition = "%s is not NULL" % key
            else:
                condition = '%s != %s' % (key, self.quote(value))
        else:
            if value.lower() == 'null':
                condition = "%s is NULL" % key
            else:
                condition = "%s = %s" % (key, self.quote(value))

        return condition

    ###########################################################
    # qdict[<table>][key_vals][<key>]
    def create_query_string(self, qdict):
        """ returns a properly formatted sql query string given a special query dictionary  """

        selectfields = []
        fromtables = []
        whereclauses = []

        print qdict

        for tablename, tabledict in qdict.items():
            fromtables.append(tablename)
            if 'select_fields' in tabledict:
                table_select_fields = tabledict['select_fields']
                if type(table_select_fields) is not list:
                    table_select_fields = table_select_fields.lower().replace(' ', '').split(',')

                if 'all' in table_select_fields:
                    selectfields.append("%s.*" % (tablename))
                else:
                    for field in table_select_fields:
                        selectfields.append("%s.%s" % (tablename, field))

            if 'key_vals' in tabledict:
                for key, val in tabledict['key_vals'].items():
                    whereclauses.append(self.make_where_clause('%s.%s' % (tablename, key), val))

            if 'join' in tabledict:
                for j in tabledict['join'].lower().split(','):
                    pat_key_val = "^\s*([^=]+)(\s*=\s*)(.+)\s*$"
                    pat_match = re.search(pat_key_val, j)
                    if pat_match is not None:
                        key = pat_match.group(1)
                        if '.' in key:
                            (jtable, key) = key.split('.')
                        else:
                            jtable = tablename

                        val = pat_match.group(3).strip()
                        whereclauses.append('%s.%s=%s' % (jtable, key, val))

        query = "SELECT %s FROM %s WHERE %s" % \
            (','.join(selectfields),
             ','.join(fromtables),
             ' AND '.join(whereclauses))
        return query

    def get_metadata(self):
        sql = "select * from ops_metadata"
        curs = self.cursor()
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]

        result = OrderedDict()
        for line in curs:
            d = dict(zip(desc, line))
            headername = d['file_header_name'].lower()
            columnname = d['column_name'].lower()
            if headername not in result:
                result[headername] = OrderedDict()
            if columnname not in result[headername]:
                result[headername][columnname] = d
            else:
                raise Exception("Found duplicate row in metadata (%s, %s)" % (headername, columnname))

        curs.close()
        return result

    def get_all_filetype_metadata(self):
        """
        Gets a dictionary of dictionaries or string=value pairs representing
        data from the OPS_METADATA, OPS_FILETYPE, and OPS_FILETYPE_METADATA tables.
        This is intended to provide a complete set of filetype metadata required
        during a run.
        Note that the returned dictionary is nested based on the order of the
        columns in the select clause.  Values in columns contained in the
        "collections" list will be turned into dictionaries keyed by the value,
        while the remaining columns will become "column_name=value" elements
        of the parent dictionary.  Thus the sql query and collections list can be
        altered without changing the rest of the code.
        Note that the code expects file_header_name, position, and column_name
        to be the final three columns in the select list. Those should not be altered.
        """
        sql = """select f.filetype,f.metadata_table,nvl(fm.file_hdu,'PRIMARY') file_hdu,
                    fm.status,fm.derived,fm.file_header_name,m.position,m.column_name
                from OPS_METADATA m, OPS_FILETYPE f, OPS_FILETYPE_METADATA fm
                where m.file_header_name=fm.file_header_name
                    and f.filetype=fm.filetype
                order by 1,2,3,4,5,6 """
        collections = ['filetype', 'file_hdu', 'status', 'derived']
        curs = self.cursor()
        curs.execute(sql)
        desc = [d[0].lower() for d in curs.description]
        result = OrderedDict()

        for row in curs:
            ptr = result
            for col, value in enumerate(row):
                normvalue = str(value).lower()
                if col >= (len(row)-3):
                    if normvalue not in ptr:
                        ptr[normvalue] = str(row[col+2]).lower()
                    else:
                        ptr[normvalue] += "," + str(row[col+2]).lower()
                    break
                if normvalue not in ptr:
                    if desc[col] in collections:
                        ptr[normvalue] = OrderedDict()
                    else:
                        ptr[desc[col]] = normvalue
                if desc[col] in collections:
                    ptr = ptr[normvalue]
        curs.close()
        return result

    def get_site_info(self):
        """ Return contents of ops_site and ops_site_val tables """
        # assumes foreign key constraints so cannot have site in ops_site_val that isn't in ops_site

        site_info = self.query_results_dict('select * from ops_site', 'name')

        sql = "select name,key,val from ops_site_val"
        curs = self.cursor()
        curs.execute(sql)
        for (name, key, val) in curs:
            site_info[name][key] = val
        return site_info

    def get_archive_info(self):
        """ Return contents of ops_archive and ops_archive_val tables """
        # assumes foreign key constraints so cannot have archive in ops_archive_val that isn't in ops_archive

        archive_info = self.query_results_dict('select * from ops_archive', 'name')

        sql = "select name,key,val from ops_archive_val"
        curs = self.cursor()
        curs.execute(sql)
        for (name, key, val) in curs:
            archive_info[name][key] = val
        return archive_info

    def get_archive_transfer_info(self):
        """ Return contents of ops_archive_transfer and ops_archive_transfer_val tables """

        archive_transfer = OrderedDict()
        sql = "select src,dst,transfer from ops_archive_transfer"
        curs = self.cursor()
        curs.execute(sql)
        for row in curs:
            if row[0] not in archive_transfer:
                archive_transfer[row[0]] = OrderedDict()
            archive_transfer[row[0]][row[1]] = OrderedDict({'transfer': row[2]})

        sql = "select src,dst,key,val from ops_archive_transfer_val"
        curs = self.cursor()
        curs.execute(sql)
        for row in curs:
            if row[0] not in archive_transfer:
                miscutils.fwdebug(
                    0, 'DESDBI_DEBUG', "WARNING: found info in ops_archive_transfer_val for src archive %s which is not in ops_archive_transfer" % row[0])
                archive_transfer[row[0]] = OrderedDict()
            if row[1] not in archive_transfer[row[0]]:
                miscutils.fwdebug(
                    0, 'DESDBI_DEBUG', "WARNING: found info in ops_archive_transfer_val for dst archive %s which is not in ops_archive_transfer" % row[1])
                archive_transfer[row[0]][row[1]] = OrderedDict()
            archive_transfer[row[0]][row[1]][row[2]] = row[3]
        return archive_transfer

    def get_job_file_mvmt_info(self):
        """ Return contents of ops_job_file_mvmt and ops_job_file_mvmt_val tables """
        # [site][home][target][key] = [val]  where req key is mvmtclass

        sql = "select site,home_archive,target_archive,mvmtclass from ops_job_file_mvmt"
        curs = self.cursor()
        curs.execute(sql)
        info = OrderedDict()
        for (site, home, target, mvmt) in curs:
            if home is None:
                home = 'no_archive'

            if target is None:
                target = 'no_archive'

            if site not in info:
                info[site] = OrderedDict()
            if home not in info[site]:
                info[site][home] = OrderedDict()
            info[site][home][target] = OrderedDict({'mvmtclass': mvmt})

        sql = "select site,home_archive,target_archive,key,val from ops_job_file_mvmt_val"
        curs = self.cursor()
        curs.execute(sql)
        for (site, home, target, key, val) in curs:
            if home is None:
                home = 'no_archive'

            if target is None:
                target = 'no_archive'

            if (site not in info or
                home not in info[site] or
                    target not in info[site][home]):
                miscutils.fwdie("Error: found info in ops_job_file_mvmt_val (%s, %s, %s, %s, %s) which is not in ops_job_file_mvmt" % (
                    site, home, target, key, val), 1)
            info[site][home][target][key] = val
        return info

    def load_filename_gtt(self, filelist):
        """ insert filenames into filename global temp table to use in join for later query """
        # returns filename GTT table name

        # make sure table is empty before loading it
        self.empty_gtt(DB_GTT_FILENAME)

        colmap = [DB_COL_FILENAME, DB_COL_COMPRESSION]
        rows = []
        for file in filelist:
            fname = None
            comp = None
            if isinstance(file, basestring):
                (fname, comp) = miscutils.parse_fullname(file, CU_PARSE_FILENAME | CU_PARSE_EXTENSION)
            elif isinstance(file, dict) and (DB_COL_FILENAME in file or DB_COL_FILENAME.lower() in file):
                if DB_COL_COMPRESSION in file:
                    fname = file[DB_COL_FILENAME]
                    comp = file[DB_COL_COMPRESSION]
                elif DB_COL_COMPRESSION.lower() in file:
                    fname = file[DB_COL_FILENAME.lower()]
                    comp = file[DB_COL_COMPRESSION.lower()]
                elif DB_COL_FILENAME in file:
                    (fname, comp) = miscutils.parse_fullname(
                        file[DB_COL_FILENAME], CU_PARSE_FILENAME | CU_PARSE_EXTENSION)
                else:
                    (fname, comp) = miscutils.parse_fullname(
                        file[DB_COL_FILENAME.lower()], CU_PARSE_FILENAME | CU_PARSE_EXTENSION)
            else:
                raise ValueError("Invalid entry filelist (%s)" % file)
            rows.append({DB_COL_FILENAME: fname, DB_COL_COMPRESSION: comp})
        self.insert_many(DB_GTT_FILENAME, colmap, rows)
        return DB_GTT_FILENAME

    def empty_gtt(self, tablename):
        """ clean out temp table for when one wants separate commit/rollback control """
        # could be changed to generic empty table function, for now wanted safety check

        if 'gtt' not in tablename.lower():
            raise ValueError("Invalid table name for a global temp table (missing GTT)")

        sql = "delete from %s" % tablename
        curs = self.cursor()
        curs.execute(sql)
        curs.close()

    def create_task(self, name, info_table,
                    parent_task_id=None, root_task_id=None, i_am_root=False,
                    label=None, do_begin=False, do_commit=False):
        """ insert a row into the task table and return task id """

        row = {'name': name, 'info_table': info_table}

        row['id'] = self.get_seq_next_value('task_seq') # get task id

        if parent_task_id is not None:
            row['parent_task_id'] = int(parent_task_id)

        if i_am_root:
            row['root_task_id'] = row['id']
        elif root_task_id is not None:
            row['root_task_id'] = int(root_task_id)

        if label is not None:
            row['label'] = label

        self.basic_insert_row('task', row)

        if do_begin:
            self.begin_task(row['id'])

        if do_commit:
            self.commit()

        return row['id']

    def begin_task(self, task_id, do_commit=False):
        """ update a row in the task table with beginning of task info """

        updatevals = {'start_time': self.get_current_timestamp_str(),
                      'exec_host': socket.gethostname()}
        wherevals = {'id': task_id} # get task id

        self.basic_update_row('task', updatevals, wherevals)
        if do_commit:
            self.commit()

    def end_task(self, task_id, status, do_commit=False):
        """ update a row in the task table with end of task info """
        wherevals = {}
        wherevals['id'] = task_id

        updatevals = {}
        updatevals['end_time'] = self.get_current_timestamp_str()
        updatevals['status'] = status

        self.basic_update_row('task', updatevals, wherevals)
        if do_commit:
            self.commit()

    def get_datafile_metadata(self, filetype):
        """ Gets a dictionary of all datafile (such as XML or fits table data files) metadata for the given filetype.
            Returns a list: [target_table_name,metadata]
        """
        TABLE = 0
        HDU = 1
        ATTRIBUTE = 2
        POSITION = 3
        COLUMN = 4
        DATATYPE = 5
        FORMAT = 6

        bindstr = self.get_named_bind_string("afiletype")
        sql = """select table_name, hdu, lower(attribute_name), position, lower(column_name), datafile_datatype, data_format
                from OPS_DATAFILE_TABLE df, OPS_DATAFILE_METADATA md
                where df.filetype = md.filetype and current_flag=1 and lower(df.filetype) = lower(""" + bindstr + """)
                order by md.attribute_name, md.POSITION"""
        result = OrderedDict()
        curs = self.cursor()
        curs.execute(sql, {"afiletype": filetype})

        tablename = None
        for row in curs:
            if tablename == None:
                tablename = row[TABLE]
            if row[HDU] not in result.keys():
                result[row[HDU]] = {}
            if row[ATTRIBUTE] not in result[row[HDU]].keys():
                result[row[HDU]][row[ATTRIBUTE]] = {}
                result[row[HDU]][row[ATTRIBUTE]]['datatype'] = row[DATATYPE]
                result[row[HDU]][row[ATTRIBUTE]]['format'] = row[FORMAT]
                result[row[HDU]][row[ATTRIBUTE]]['columns'] = []
            if len(result[row[HDU]][row[ATTRIBUTE]]['columns']) == row[POSITION]:
                result[row[HDU]][row[ATTRIBUTE]]['columns'].append(row[COLUMN])
            else:
                result[row[HDU]][row[ATTRIBUTE]]['columns'][row[POSITION]] = row[COLUMN]
        curs.close()
        return [tablename, result]
