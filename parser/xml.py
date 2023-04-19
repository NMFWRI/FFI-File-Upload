import os
import logging
from uuid import uuid4

import pandas as pd
from pandas import DataFrame, concat, isna, read_sql, options, to_datetime
from re import sub, findall, match
from dateutil import parser
from sqlalchemy import exc, MetaData, Table, text, sql, select, and_, or_
from sqlalchemy.dialects.mysql import insert
from sqlalchemy.orm import Session
from parser.server import FFIDatabase
from numpy import nan
from hashlib import sha256
from parser.functions import strip_namespace, convert_datetime
import xml.etree.ElementTree as ET
import datetime

options.mode.chained_assignment = None


def insert_on_duplicate(table, conn, keys, data_iter):
    """S
    For use to df.to_sql()

    handles inserting duplicates into tables. It will pass on rows that are already in the tables
    from https://stackoverflow.com/questions/30337394/pandas-to-sql-fails-on-duplicate-primary-key
    """
    insert_stmt = insert(table.table).values(list(data_iter))
    on_duplicate_key_stmt = insert_stmt.on_duplicate_key_update(insert_stmt.inserted)
    conn.execute(on_duplicate_key_stmt)


class FFIFile:
    """
    this is a class that represents the entire XML file. It can be thought of as a collection of 'tables' represented by
    the element names that appear in the XML file.
    """

    def __init__(self, file):
        """
        parses a ElementTree root element and creates the FFIFile class
        """
        # with open(file) as open_file:
        #     f_gen = (open_file.readline() for i in range(50000))
        #     f = '\n'.join(f_gen)
        #     file_hash = sha256(f.encode())
        #     file_id = file_hash.hexdigest()

        # self._id = file_id
        self.file = file.name.strip('.xml')
        self._tree = ET.parse(file)
        self._root = self._tree.getroot()
        self._namespace = findall(r'\{http://\w+\.\w{3}[\w/.\d]+\}', self._root.tag)[0].strip('{}')
        self._base_tables = {}
        self._data_map = {}
        self._excluded = ['FuelConstants_DL', 'FuelConstants_ExpDL', 'FuelConstants_FWD', 'FuelConstants_Veg',
                          'FuelConstants_CWD', 'Schema_Version', 'Program', 'Project', 'DataGridViewSettings',
                          'MasterSpecies_LastModified', 'Settings']
        self._processed = []
        # self._tables = {}
        # self._filtered = False
        self._retry_tables = {}
        self.duplicate = False
        self.dup_on = None
        self.many_tables = False
        self.debug = []

        self.reg_unit = []
        self.project_units = []
        self.plots = []
        self.events = []

        self._parse_data()
        self.version = self['Schema_Version']['Schema_Version'][0]
        # self._parse_idents()

    def __getitem__(self, item):
        """
        I needed to create some way to index the FFIFile class, so this will pass the index to the data_map and return
        whatever that operation returns.

        e.g <FFIFile>['column'] returns <internal DataFrame>['column']
        """

        if item in self._data_map.keys():
            return self._data_map[item]
        else:
            raise KeyError('{} not in FFI XML file.'.format(item))

    @staticmethod
    def _update_last_modified(self, session):
        """
        Just updates the LastModified table with current user
        """
        
        comp_name = os.environ['COMPUTERNAME']
        user = os.environ['USERNAME']
        now = str(datetime.datetime.now())
        lm_dict = {'last_edit_date': [now],
                   'Machine_Name': [comp_name],
                   'User_Name': [f'{comp_name}\\{user}']}
        last_modified = DataFrame(lm_dict)
        last_modified.to_sql('Last_Modified_Date', session.bind, index=False, if_exists='replace')
        return last_modified

    def _parse_data(self):
        """
        Iterates through each element name that was produced in the __init__ operation. This is what actually populates
        the data_map element
        """
        # needed_tables = ['MacroPlot', 'RegistrationUnit', 'MM_ProjectUnit_MacroPlot', 'ProjectUnit', 'SampleEvent',
        #                  'MM_MonitoringStatus_SampleEvent', 'MonitoringStatus', 'MethodAttribute', 'AttributeData',
        #                  'Method', 'LU_DataType', 'Schema_Version', 'MasterSpecies', 'SampleData', 'SampleAttribute',
        #                  'LocalSpecies']

        tags = set([strip_namespace(element.tag) for element in self._root])
        for tag in tags:
            all_data = self._root.findall(tag, namespaces={'': self._namespace})
            dfs = [
                DataFrame({strip_namespace(attr.tag): [attr.text] for attr in data_set})
                for data_set in all_data
            ]
            df = concat(dfs)
            for col in df.columns:
                if '_GUID' in col:
                    df[col] = df[col].apply(lambda row: row.upper())
                elif 'Date' in col or 'Time' in col:
                    df[col] = df[col].apply(lambda row: convert_datetime(row))
            self._data_map[strip_namespace(tag)] = df

    def _parse_idents(self):
        """
        Parses all the major identifiers out for help with duplicate processing. Unsure if this is needed yet, but at
        least it's built out if it is.

        Produces a list of dicts with corresponding GUIDs and non-guid identifiers for each instance of each primary
        identifying
        """

        # TODO: transform below block to function

        # the name is an appropriate unique identifier for admin units
        reg_unit = self['RegistrationUnit']
        reg_guids = list(reg_unit['RegistrationUnit_GUID'])
        reg_names = list(reg_unit['RegistrationUnit_Name'])
        self.reg_unit.append({'guid': reg_guids[0], 'name': reg_names[0]})  # only one admin unit per file

        # name is also used for projects
        proj_units = self['ProjectUnit']
        proj_guids = list(proj_units['ProjectUnit_GUID'])
        proj_names = list(proj_units['ProjectUnit_Name'])
        proj_dicts = [{'guid': z[0], 'name':z[1]}
                      for z in zip(proj_guids, proj_names)]  # there can be multiple projects
        self.project_units = proj_dicts

        # since we're dealing with one admin unit, names are sufficient for plots
        plot_units = self['MacroPlot']
        plot_guids = list(plot_units['MacroPlot_GUID'])
        plot_names = list(plot_units['MacroPlot_Name'])
        plot_dicts = [{'guid': z[0], 'name': z[1]}
                      for z in zip(plot_guids, plot_names)]  # again, multiple plots
        self.plots = plot_dicts

        # slightly trickier - both date and plot name serve as a truly unique identification method for events
        event_units = self['SampleEvent']\
            .merge(self['MacroPlot'], left_on='SampleEvent_Plot_GUID', right_on='MacroPlot_GUID', how='left')
        event_guids = list(event_units['SampleEvent_GUID'])
        e_dates = list(event_units['SampleEvent_Date'])
        event_dates = [convert_datetime(date) for date in e_dates]
        plots = list(event_units['MacroPlot_Name'])
        event_dicts = [{'guid': z[0], 'datetime': z[1], 'plot': z[2]}
                       for z in zip(event_guids, event_dates, plots)]  # we need to include it all just in case
        self.events = event_dicts

    def _attr_to_many(self):
        """
        Converts the AttributeData and AttributeRow tables into the many-tables format used by FFIMT
        """

        # These first few blocks are self-explanatory
        select_list = ['AttributeRow_DataRow_GUID','SampleRow_Original_GUID', 'MethodAtt_FieldName',
                       'AttributeData_Value', 'AttributeRow_Original_GUID', 'AttributeRow_CreatedBy',
                       'AttributeRow_CreatedDate', 'AttributeRow_ModifiedBy', 'AttributeRow_ModifiedDate',
                       'Method_Name', 'Method_UnitSystem']

        select_rename = {'AttributeRow_DataRow_GUID': 'AttributeData_DataRow_GUID',
                         'SampleRow_Original_GUID': 'AttributeData_SampleRow_GUID',
                         'AttributeRow_Original_GUID': 'AttributeData_Original_GUID',
                         'AttributeRow_CreatedBy': 'AttributeData_CreatedBy',
                         'AttributeRow_CreatedDate': 'AttributeData_CreatedDate',
                         'AttributeRow_ModifiedBy': 'AttributeData_ModifiedBy',
                         'AttributeRow_ModifiedDate': 'AttributeData_ModifiedDate'}

        attr_data = self['AttributeRow'] \
            .merge(self['AttributeData'],
                   left_on='AttributeRow_ID',
                   right_on='AttributeData_DataRow_ID', how='left') \
            .merge(self['MethodAttribute'],
                   left_on='AttributeData_MethodAtt_ID',
                   right_on='MethodAtt_ID', how='left') \
            .merge(self['Method'],
                   left_on='MethodAtt_Method_GUID',
                   right_on='Method_GUID', how='left') \
            .merge(self['SampleRow'],
                   left_on='AttributeData_SampleRow_ID',
                   right_on='SampleRow_ID', how='left')
        try:
            attr_select = attr_data[select_list]
        except KeyError:  # these fields are in the SQL tables, but aren't included in the XML
            # I can probably get rid of this, but I'm not sure how the indexing and renaming would work, so I'll
            attr_data['AttributeRow_CreatedBy'] = pd.NA
            attr_data['AttributeRow_CreatedDate'] = pd.NA
            attr_data['AttributeRow_ModifiedBy'] = pd.NA
            attr_data['AttributeRow_ModifiedDate'] = pd.NA
            attr_select = attr_data[select_list]

        attr_long = attr_select.rename(columns=select_rename)  # renaming columns
        methods = attr_long['Method_Name'].unique()
        for method in methods:
            temp = attr_long.loc[attr_long['Method_Name'] == method]
            subset = temp.pivot(index=['AttributeData_DataRow_GUID', 'AttributeData_SampleRow_GUID',
                                       'AttributeData_CreatedBy', 'AttributeData_CreatedDate',
                                       'AttributeData_ModifiedBy', 'AttributeData_ModifiedDate',
                                       'Method_UnitSystem'],
                                columns=['MethodAtt_FieldName'],
                                values='AttributeData_Value').reset_index()
            unit_systems = subset['Method_UnitSystem'].unique()
            table_name = method.replace(' ', '').replace('-', '_').replace('(', '_').replace(')', '_').strip('_')
            if len(unit_systems) > 1:
                for unit_system in unit_systems:
                    unit_subset = subset.loc[subset['Method_UnitSystem'] == unit_system]
                    if unit_system != 'English':
                        sql_table = f"{table_name}_{unit_system}_Attribute"
                    else:
                        sql_table = f"{table_name}_Attribute"
                    self._data_map[sql_table] = unit_subset
            else:
                sql_table = f"{table_name}_Attribute"
                subset.drop(columns=['Method_UnitSystem'], axis=1, inplace=True)
                self._data_map[sql_table] = subset

    def _sample_to_many(self):
        select_list = ['SampleRow_Original_GUID', 'SampleData_SampleEvent_GUID', 'SampleAtt_FieldName',
                       'SampleData_Value', 'SampleRow_CreatedBy', 'SampleRow_CreatedDate', 'SampleRow_ModifiedBy',
                       'SampleRow_ModifiedDate', 'Method_Name', 'Method_UnitSystem']
        select_rename = {'SampleRow_Original_GUID': 'SampleData_SampleRow_GUID',
                         'SampleRow_CreatedBy': 'SampleData_CreatedBy',
                         'SampleRow_CreatedDate': 'SampleData_CreatedDate',
                         'SampleRow_ModifiedBy': 'SampleData_ModifiedBy',
                         'SampleRow_ModifiedDate': 'SampleData_ModifiedDate'}

        sample_data = self['SampleRow'] \
            .merge(self['SampleData'],
                   left_on='SampleRow_ID',
                   right_on='SampleData_SampleRow_ID', how='left') \
            .merge(self['SampleAttribute'],
                   left_on='SampleData_SampleAtt_ID',
                   right_on='SampleAtt_ID', how='left')\
            .merge(self['Method'],
                   left_on='SampleAtt_Method_GUID',
                   right_on='Method_GUID', how='left')
        try:
            sample_select = sample_data[select_list]
        except KeyError:
            sample_data['SampleRow_CreatedBy'] = pd.NA
            sample_data['SampleRow_CreatedDate'] = pd.NA
            sample_data['SampleRow_ModifiedBy'] = pd.NA
            sample_data['SampleRow_ModifiedDate'] = pd.NA
            sample_select = sample_data[select_list]

        sample_long = sample_select.rename(columns=select_rename)
        sample_long['SampleData_Original_GUID'] = sample_long.apply(lambda _: str(uuid4()).upper())
        methods = sample_long['Method_Name'].unique()
        for method in methods:
            temp = sample_long.loc[sample_long['Method_Name'] == method]
            subset = temp.pivot(index=['SampleData_SampleRow_GUID', 'SampleData_SampleEvent_GUID',
                                       'SampleData_Original_GUID', 'SampleData_CreatedBy',
                                       'SampleData_CreatedDate', 'SampleData_ModifiedBy',
                                       'SampleData_ModifiedDate', 'Method_UnitSystem'],
                                columns=['SampleAtt_FieldName'],
                                values='SampleData_Value').reset_index()
            unit_systems = subset['Method_UnitSystem'].unique()
            table_name = method.replace(' ', '').replace('-', '_').replace('(', '_').replace(')', '_').strip('_')
            if len(unit_systems) > 1:
                for unit_system in unit_systems:
                    unit_subset = subset.loc[subset['Method_UnitSystem'] == unit_system]
                    unit_subset.drop(columns=['Method_UnitSystem'], axis=1, inplace=True)
                    if unit_system != 'English':
                        sql_table = f"{table_name}_{unit_system}_Sample"
                    else:
                        sql_table = f"{table_name}_Sample"
                    self._data_map[sql_table] = unit_subset
            else:
                sql_table = f"{table_name}_Sample"
                subset.drop(columns=['Method_UnitSystem'], axis=1, inplace=True)
                self._data_map[sql_table] = subset

    def to_many_tables(self):
        print('Pivoting Attribute data')
        self._attr_to_many()
        del self._data_map['AttributeRow']
        del self._data_map['AttributeData']

        print('Pivoting Sample data')
        self._sample_to_many()
        del self._data_map['SampleData']
        del self._data_map['SampleRow']

        self.many_tables = True

    def check_dups(self, ffi_server: FFIDatabase):
        tables = {'admin_unit': ffi_server.tables['RegistrationUnit'],
                  'project': ffi_server.tables['ProjectUnit'],
                  'plot': ffi_server.tables['MacroPlot'],
                  'event': ffi_server.tables['SampleEvent']}

        with Session(ffi_server.engine) as sesh:

            print("Generating queries")
            queries = {'admin_unit': sesh.query(tables['admin_unit'])
                .filter(tables['admin_unit'].c['RegistrationUnit_Name'].in_([x['name'] for x in self.reg_unit])),

                       'project': sesh.query(tables['project'])
                           .filter(tables['project'].c['ProjectUnit_Name'].in_([x['name'] for x in self.project_units])),

                       'plot': sesh.query(tables['plot'])
                           .filter(tables['plot'].c['MacroPlot_Name'].in_([x['name'] for x in self.plots])),

                       'event': sesh.query(tables['event'])
                           .filter(tables['event'].c['SampleEvent_Date'].in_([x['datetime'] for x in self.events]))}

            print("Gathering duplicates")
            dup_dfs = {'admin_unit': pd.read_sql(queries['admin_unit'].statement, sesh.bind),
                       'project': pd.read_sql(queries['project'].statement, sesh.bind),
                       'plot': pd.read_sql(queries['plot'].statement, sesh.bind),
                       'event': pd.read_sql(queries['event'].statement, sesh.bind)}

            dups = {'admin_unit': list(dup_dfs['admin_unit']['RegistrationUnit_Name']),
                    'project': list(dup_dfs['project']['ProjectUnit_Name']),
                    'plot': list(dup_dfs['plot']['MacroPlot_Name']),
                    'event': list(dup_dfs['event']['SampleEvent_Date'])}
            dups['event'] = [parser.parse(str(sample)) for sample in dups['event']]

            print("Generating non-duplicates")
            new_data = {'admin_unit': [x for x in self.reg_unit if x['name'] not in dups['admin_unit']],
                        'project': [x for x in self.project_units if x['name'] not in dups['project']],
                        'plot': [x for x in self.plots if x['name'] not in dups['plot']],
                        'event': [x for x in self.events if parser.parse(x['datetime']) not in dups['event']]}

            if len(new_data['admin_unit']) == 0 and \
                    len(new_data['project']) == 0 and \
                    len(new_data['plot']) == 0 and \
                    len(new_data['event']) == 0:

                self.duplicate = True
            else:
                self.dup_on = 'Partial'
                print(f"Duplicate admin units: {dups['admin_unit']}\n"
                      f"Duplicate projects: {dups['project']}\n"
                      f"Duplicate plots: {dups['plot']}\n"
                      f"Duplicate events: {dups['event']}")

    def _insert_into_db(self, ffi_db, table):
        """
        Checks foreign key constraints and inserts any necessary tables first; checks for existing primary keys
        in database, filters existing primary keys out of the current data tables; and finally inserts the table
        into the database. The function is recursive to ensure that all foreign key constraints are satisfied (the
        foreign key needs to exist in the foreign table before data can be inserted).
        """

        print(f'\nDuplicate checking for {table}')

        filtered_table = DataFrame()
        pks = ffi_db.get_primary_keys()
        fks = ffi_db.get_foreign_keys()

        table_fks = fks[table]
        table_pks = pks[table]
        multi_pk = len(table_pks) > 1

        # we need to ensure that the tables on which there are foreign key constraints are entered before we upload
        # new data to the current table. This will produce a recursive pattern to insert all dependencies first.
        if len(table_fks) > 0:
            for const in table_fks:
                const_list = table_fks[const]
                for tup in const_list:
                    add_table = tup[0]
                    if add_table not in self._processed and \
                            add_table in self._data_map:
                        print(f'Adding foreign key dependency: {add_table}')
                        self._insert_into_db(ffi_db, add_table)

        xml_table = self[table]
        for k in table_pks:
            if 'GUID' not in k and 'ID' in k:  # we need to make sure the types are preserved
                xml_table[k] = xml_table[k].astype('int64')

        db_table = ffi_db.tables[table]
        key_cols = xml_table[table_pks]
        key_tuple = key_cols.itertuples(index=False)  # Gathers each key pair
        keys = [k._asdict() for k in key_tuple]  # Why is this a protected function?? who knows. it's in the doc

        with ffi_db.start_session() as sesh:
            table_cols = [db_table.c[col] for col in table_pks]
            selected = select(*table_cols)
            filters = []

            # Create nested equality conditions for each set of primary keys
            for key in keys:
                cols = key.keys()
                sub_filter = []
                for col in cols:
                    expr = (db_table.c[col] == key[col])  # stores a SqlAlchemy binary expression
                    sub_filter.append(expr)
                filters.append(sub_filter)

            if multi_pk:  # Creates the composite primary key condition
                wheres = [and_(*key_group) for key_group in filters]
            else:       # simple equality statements for single key identifiers
                wheres = [f[0] for f in filters]

            if (key_size := len(wheres)) > 100:
                merge_df = []
                count = 0
                while count < key_size:
                    up_bound = count + 100
                    batch = wheres[count:up_bound]
                    query = selected.where(or_(*batch))
                    batch_df = pd.read_sql(query, sesh.bind)
                    merge_df.append(batch_df)
                    count = up_bound
                dup_df = pd.concat(merge_df)

            else:
                query = selected.where(or_(*wheres))  # unpack where conditions above into a query
                dup_df = pd.read_sql(query, sesh.bind)

            # need to do something similar to above to filter out duplicate rows
            old_tup = dup_df.itertuples(index=False)  # Gathers each key pair
            old_keys = [k._asdict() for k in old_tup]
            filtered_table = xml_table

            for key in old_keys:
                str_list = []
                # creates a set of queries to filter duplicates out of the DataFrame
                for col, val in key.items():
                    if 'GUID' not in col and 'ID' in col:
                        f_string = f'{col} != {val}'
                    else:
                        f_string = f'{col} != \'{val}\''
                    str_list.append(f_string)
                key_filter = ' | '.join(str_list)
                filtered_table = filtered_table.query(key_filter)

        if len(filtered_table) > 0:
            ident = True
            with ffi_db.start_session() as sesh:
                try:  # some tables have this constraint, some don't. But we need to turn it on if it does.
                    sesh.execute(text(f'SET IDENTITY_INSERT {table} ON'))
                    sesh.commit()
                except exc.ProgrammingError:
                    ident = False

            with ffi_db.start_session() as sesh:
                print(f'Attempting to write {table} to database.')
                try:
                    filtered_table.to_sql(table, sesh.bind, if_exists='append', index=False)
                except exc.DataError as e:
                    print(e)
                    print('Skipping.')
                    pass
                self._processed.append(table)
                self._update_last_modified(self, sesh)
                print(f'Wrote {len(filtered_table)} lines of {table} to database.')

            with ffi_db.start_session() as sesh:
                if ident:
                    sesh.execute(text(f'SET IDENTITY_INSERT {table} OFF'))
        else:
            print(f'\nNo new data to add for {table}.')

    def tables_to_db(self, ffi_db):
        """
        Iterates through each table in the data map and inserts it into the database
        """
        print(f'Inserting data for {self.file}')
        for table in self._data_map:
            if table not in self._excluded:
                self._insert_into_db(ffi_db, table)

    def tables_to_csv(self):

        if not os.path.isdir('csv'):
            os.mkdir('csv')

        for table in self._data_map:
            df = self._data_map[table]
            df.to_csv(f'csv/{table}.csv')



