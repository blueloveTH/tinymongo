import keyword
import pickle as pkl
import base64
from datetime import datetime

from tinymongo.columns import COLUMN_TYPES
from tinymongo.db import Database

def export(self: 'Database') -> str:
    metadata = base64.b64encode(pkl.dumps(self, protocol=4)).decode()

    all_row_types = {}
    for table in self.tables.values():
        all_row_types[table] = table.name.capitalize() + 'Row'

    db_tables = []
    for table in self.tables.values():
        db_tables.append(f"    {table.name}: 'Table[{all_row_types[table]}]'")
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

    def dereference(self, dbref: str) -> {' | '.join(all_row_types.values())!r}:
        table, id = dbref[1:].split(':')
        return self.tables[table].get_by_id(id)

    @property
    def name(self):
        return {self.name!r}

class Table(Generic[T]):
    def __init__(self, name: str, index: list[int], data: list):
        assert len(index) == len(data)
        self.name = name
        self.indexed_data = {{i: row for i, row in zip(index, data)}}
        self.data = data
        
    def __len__(self):
        return len(self.data)

    def __iter__(self):
        return iter(self.data)

    def __getitem__(self, i: int) -> 'T':
        return self.data[i]

    def get_by_id(self, id: int) -> 'T':
        return self.indexed_data[id]
''']
    
    for table in self.tables.values():  
        src.append(
f'''
@dataclass
class {all_row_types[table]}(Table):
''')
        empty = True
        for i, col_type_name in enumerate(table.column_types.values()):
            if i == 0:
                continue
            col_type = COLUMN_TYPES[col_type_name]
            col_name = table.df.columns[i]
            if keyword.iskeyword(col_name):
                col_name += '_'

            if col_type_name == 'dbref':
                ref_col_name = '_dbref__' + col_name
                src.append(f'    {ref_col_name}: str\n')
                src.append(f'''
    @property
    def {col_name}(self):
        return db.dereference(self.{ref_col_name})
''')
            else:
                src.append(f'    {col_name}: {col_type.name}\n')

            empty = False

        if empty:
            src.append('    pass\n')
    
    src.append('\n\ndb = Database()\n')
    
    for table in self.tables.values():
        data_repr = ['[\n']
        for row in table.df.itertuples(index=False):
            data_repr.append('    ')
            data_repr.append(all_row_types[table] + repr(tuple(row[1:])))
            data_repr.append(',\n')
        data_repr.append(']')
        src.append(f'''
db.{table.name} = db.tables[{table.name!r}] = Table({table.name!r}, {''.join(data_repr)})
''')
    return ''.join(src)