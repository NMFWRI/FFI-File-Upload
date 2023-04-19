import configparser
import sys
import os
import re
import uuid
import pandas as pd
from sqlalchemy import create_engine, inspect, exc
from parser.xml import *
from parser.functions import create_url
from parser.server import FFIDatabase


def main():
    # Fill this in before running!!!!
    path = 'C:/Users/Corey/OneDrive/OneDrive - New Mexico Highlands University/2023_Admin_Exports_For_Conversion/1.05.13.00/'
    # path = 'C:/Users/Corey/OneDrive/OneDrive - New Mexico Highlands University/Data/FFI Data/test'
    debug = False
    # debug = True

    # users need to create their own local config file (see README)
    config = configparser.ConfigParser()
    config.read('config.ini')

    sql_config = config['SQLServer2']
    sql_url = create_url(**sql_config)
    sql_engine = create_engine(sql_url)
    server = FFIDatabase(sql_engine)

    if not os.path.isdir(processed := os.path.join(path, 'processed')):
        os.mkdir(processed)

    xml_files = [f for f in os.scandir(path) if f.is_file() and '.xml' in f.path]

    for export in xml_files:

        file = export.path
        print(f'\nReading in {export}')
        ffi_data = FFIFile(export)

        if debug:
            new_map = {'MethodAttributeCode': ffi_data['MethodAttributeCode']}
            ffi_data._data_map = new_map
            ffi_data.version = '1'

        if '1.05.13' in ffi_data.version or '1.05.08' in ffi_data.version:
            print(f'Converting to MT format.')
            ffi_data.to_many_tables()

        ffi_data.tables_to_db(server)

        os.rename(file, os.path.join(processed, export))


if __name__ == "__main__":
    main()
