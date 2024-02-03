import pandas as pd
from tinymongo.columns import COLUMN_TYPES, ColumnType

class Table:
    column_types: dict[str, str]

    def __init__(self, db: 'Database', name: str):
        self.db = db
        self.name = name
        self.df = pd.DataFrame(columns=['?'])
        self.column_types = {'?': 'bool'}
        self._next_id = 1000
    
    def get_column_type(self, col_name: str) -> 'ColumnType':
        return COLUMN_TYPES[self.column_types[col_name]]

    def next_id(self):
        self._next_id += 1
        return self._next_id


class Database:
    name: str
    tables: dict[str, Table]

    current_table: Table | None

    def __init__(self, name: str):
        self.name = name
        self.tables = {}
        self.current_table = None

    def create_table(self, table_name: str):
        table = Table(self, table_name)
        self.tables[table_name] = table

    def set_current_table(self, table_name: str | None):
        self.current_table = self.tables.get(table_name)
  