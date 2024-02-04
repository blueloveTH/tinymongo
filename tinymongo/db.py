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
  
    def check_integrity(self) -> tuple[bool, str]:
        for table in self.tables.values():
            for col in table.df.columns:
                col_type = table.get_column_type(col)
                if col_type.name != 'dbref':
                    continue
                for rowi, value in enumerate(table.df[col], start=1):
                    try:
                        assert isinstance(value, str)
                        assert value[0] == '^'
                        _0, _1 = value[1:].split(':')
                        assert _0 in self.tables
                        assert int(_1) in self.tables[_0].df.index
                    except:
                        return False, (value, table.name, rowi)
        return True, None