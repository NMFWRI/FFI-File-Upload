import pandas as pd
from sqlalchemy import MetaData
from sqlalchemy.orm import Session


class FFIDatabase:
    """
    this represents everything you will need from an FFI database
    """

    def __init__(self, engine):
        self.engine = engine
        self.meta = MetaData()
        self.meta.reflect(self.engine)
        self.tables = self.meta.tables
        self._primary_keys = None
        self._foreign_keys = None

    def get_primary_keys(self):
        if not self._primary_keys:
            pks = {table: [column.name for column in self.tables[table].primary_key.columns]
                             for table in self.tables}
            self._primary_keys = pks

        return self._primary_keys

    def get_foreign_keys(self):
        if not self._foreign_keys:
            fks = {table: {column.name: [(fk.column.table.name, fk.column.name)
                                         for fk in column.foreign_keys]
                           for constraint in list(self.tables[table].foreign_key_constraints)
                           for column in constraint.columns
                           }
                   for table in self.tables}
            self._foreign_keys = fks

        return self._foreign_keys

    def start_session(self):
        return Session(self.engine)

