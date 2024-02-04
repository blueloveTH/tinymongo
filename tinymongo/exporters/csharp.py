import keyword
import pickle as pkl
import base64
import json
from datetime import datetime

from tinymongo.columns import COLUMN_TYPES
from tinymongo.db import Database

def to_json(value):
    if isinstance(value, float):
        return '(float)' + repr(value)
    return json.dumps(value, ensure_ascii=False)

def export(self: 'Database') -> str:
    metadata = base64.b64encode(pkl.dumps(self, protocol=4)).decode()

    all_row_types = {}
    for table in self.tables.values():
        all_row_types[table] = table.name.capitalize() + 'Row'

    db_tables = []
    for table in self.tables.values():
        db_tables.append(f"        public Table<{all_row_types[table]}> {table.name};")

    # data
    data_src = []
    for table in self.tables.values():
        table_name_json = to_json(table.name)
        data_repr = [f'new List<{all_row_types[table]}>{{' + '\n']
        for row in table.df.itertuples(index=True):
            row = list(row); del row[1]
            data_repr.append('    ')
            data_repr.append('new ' + all_row_types[table] + '(' + ', '.join(map(to_json, row)) + ')')
            data_repr.append(',\n')
        data_repr[-1] = '\n'    # remove last comma
        data_repr.append('}')
        data_src.append(f'''_instance.tables[{table_name_json}] = _instance.{table.name} = new Table<{all_row_types[table]}>({table_name_json}, {''.join(data_repr)});\n''')
    data_src = [f'            {line}' for line in '\n'.join(data_src).splitlines()]

    src = [f'''// {metadata}      
// Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

using System;
using System.Collections.Generic;

namespace TinyMongo.{self.name}
{{
    public class Database
    {{
{chr(10).join(db_tables)}

        public Dictionary<string, object> tables = new Dictionary<string, object>();

        private static Database _instance;

        public static Database instance {{ get {{
            if (_instance != null) return _instance;
            _instance = new Database();
{chr(10).join(data_src)}
            return _instance;
        }} }}

        public object Dereference(string dbref)
        {{
            var parts = dbref.Substring(1).Split(':');
            var table = parts[0];
            var id = int.Parse(parts[1]);
            object ret = ((ITable)tables[table]).WithId(id);
            if(ret == null) throw new Exception("DBRef not found: " + dbref);
            return ret;
        }}

        public string name => {to_json(self.name)};
        public int Count => tables.Count;
        public object this[string name] => tables[name];
    }}

    public interface ITable
    {{
        public object WithId(int id);
    }}

    public interface IRow
    {{
        public int GetId();
    }}

    public class Table<T>: ITable where T: IRow
    {{
        public string name;
        public List<T> data;
        public Dictionary<int, T> indexedData;

        public Table(string name, List<T> data)
        {{
            this.name = name;
            this.data = data;
            this.indexedData = new Dictionary<int, T>();
            foreach (var row in data) this.indexedData.Add(row.GetId(), row);
        }}

        object ITable.WithId(int id) => indexedData.TryGetValue(id, out var row) ? row : null;

        public int Count => data.Count;
        public T this[int i] => data[i];
    }}

''']
    
    for table in self.tables.values():
        src.append(f'    public class {all_row_types[table]}: IRow\n')
        src.append('    {\n')

        variables = {'id': 'int'}
        for i, col_type_name in enumerate(table.column_types.values()):
            if i == 0:
                src.append(f'        public int id;\n')
                continue
            col_type = COLUMN_TYPES[col_type_name]
            col_name = table.df.columns[i]
            if keyword.iskeyword(col_name):
                col_name += '_'

            cs_dtypes = {
                'int': 'int',
                'float': 'float',
                'str': 'string',
                'bool': 'bool',
                'dbref': 'string'
            }

            if col_type_name == 'dbref':
                ref_col_name = '_dbref__' + col_name
                src.append(f'        public string {ref_col_name};\n')
                src.append(f'        public object {col_name} => Database.instance.Dereference(this.{ref_col_name});\n')
                variables[ref_col_name] = cs_dtypes[col_type_name]
            else:
                src.append(f'        public {cs_dtypes[col_type_name]} {col_name};\n')
                variables[col_name] = cs_dtypes[col_type_name]

        src.append(f'\n        public {all_row_types[table]}({", ".join(f"{v} {k}" for k, v in variables.items())})\n')
        src.append('        {\n')
        for k, v in variables.items():
            src.append(f'            this.{k} = {k};\n')
        src.append('        }\n')

        src.append('        public int GetId() => this.id;\n')
        src.append('    }\n\n')
    
    src.append('}')
    return ''.join(src)