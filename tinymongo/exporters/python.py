import streamlit as st
import pandas as pd
import sys
import keyword
import pickle as pkl
import base64
from datetime import datetime

from tinymongo.columns import COLUMN_TYPES
from tinymongo.db import Database, Table

def export(self: 'Database') -> str:
        metadata = base64.b64encode(pkl.dumps(self, protocol=4)).decode()
        db_tables = []
        for table in self.tables.values():
            db_tables.append(f"    {table.name}: 'Table[{table.name.capitalize()}Row]'")
        src = [f'''# {metadata}      
# Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
               
from dataclasses import dataclass
from typing import Generic, TypeVar
               
T = TypeVar('T', bound='Table')

class Database:
{chr(10).join(db_tables)}
    tables: dict[str, 'Table']

    def __init__(self):
        self.tables = dict()

    def __len__(self):
        return len(self.tables)

    def __getitem__(self, name: str) -> 'Table':
        return self.tables[name]

    @property
    def name(self):
        return {self.name!r}
 
class Table(Generic[T]):
    def __init__(self, name: str, data: list):
        self.name = name
        self.data = data

    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return iter(self.data)

    def __getitem__(self, i: int) -> 'T':
        return self.data[i]
''']
        
        for table in self.tables.values():  
            src.append(
f'''
@dataclass
class {table.name.capitalize()}Row(Table):
''')
            
            for i, col_type_name in enumerate(table.column_types.values()):
                if i == 0:
                    continue
                col_type = COLUMN_TYPES[col_type_name]
                col_name = table.df.columns[i]
                if keyword.iskeyword(col_name):
                    col_name += '_'
                src.append(f'    {col_name}: {col_type.name}\n')
            src.append('    pass\n')
      
        src.append('\n\ndb = Database()\n')
        
        for table in self.tables.values():
            data_repr = ['[\n']
            for row in table.df.itertuples(index=False):
                data_repr.append('    ')
                data_repr.append(table.name.capitalize() + 'Row' + repr(tuple(row[1:])))
                data_repr.append(',\n')
            data_repr.append(']')
            src.append(f'''
db.{table.name} = db.tables[{table.name!r}] = Table({table.name!r}, {''.join(data_repr)})
''')
        return ''.join(src)