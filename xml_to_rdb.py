import configparser
from sqlalchemy import create_engine
from parser.xml import *
from parser.functions import create_url
from parser.server import FFIDatabase


def main():
    # Fill this in before running!!!!
    path = 'YourDataPathHere'

    # DEBUGGING
    debug = False
    # debug = True

    # users need to create their own local config file (see README)
    config = configparser.ConfigParser()
    config.read('config.ini')

    sql_config = config['NameOfYourServer']
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
            new_map = {'TableYouWantToTest': ffi_data['TableYouWantToTest']}
            ffi_data._data_map = new_map
            ffi_data.version = '1'

        if '1.05.13' in ffi_data.version or '1.05.08' in ffi_data.version:
            print(f'Converting to MT format.')
            ffi_data.to_many_tables()

        ffi_data.tables_to_db(server)
        ffi_data.remove_mm_method_problems(server)

        os.rename(file, os.path.join(processed, export))


if __name__ == "__main__":
    main()
