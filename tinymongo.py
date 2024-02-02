import json

class Database:
    tables: dict[str, 'Table']

    def __init__(self):
        self.tables = {}

        self.converters = {
            "string": self._to_string,
            "int": self._to_int,
            "float": self._to_float,
            "bool": self._to_bool,
            "dbref": self._to_dbref,
            "list": self._to_list,
            "dict": self._to_dict
        }

    def _to_string(self, value: str):
        return value
    def _to_int(self, value: str):
        return int(value)
    def _to_float(self, value: str):
        return float(value)
    def _to_bool(self, value: str):
        return bool(value)
    def _to_dbref(self, value: str):
        assert self.dereference(value)
        return value
    def _to_list(self, value: str):
        ret = json.loads(value)
        assert type(ret) is list
        return ret
    def _to_dict(self, value: str):
        ret = json.loads(value)
        assert type(ret) is dict
        return ret

    def create_table(self, name: str):
        t = Table(self, name)
        self.tables[name] = t
        return t

    def __getitem__(self, name: str):
        return self.tables[name]
    
    def dereference(self, ref: str):
        table_name, row_id = ref.split(":")
        return self.tables[table_name].get_by_id(int(row_id))
    
    def __len__(self):
        return len(self.tables)
    
    def save(self):
        return json.dumps({name: table.to_dict() for name, table in self.tables.items()})
    
    @staticmethod
    def load(data: str):
        db = Database()
        for name, table_data in json.loads(data).items():
            db.tables[name] = Table.from_dict(db, table_data)
        return db

class Table:
    db: Database
    name: str
    columns: list[tuple[str, str]]
    rows: list
    next_id: int

    _column_indices: dict[str, int]
    _indices: dict[str, dict]

    def __init__(self, db: Database, name: str):
        self.db = db
        self.name = name
        self.columns = [("id", "int")]
        self.rows = []
        self.next_id = 100000

        self._column_indices = {"id": 0}
        self._indices = {"id": {}}

    def rebuild_indices(self):
        # rebuild column indices
        self._column_indices.clear()
        for i in range(len(self.columns)):
            self._column_indices[self.columns[i][0]] = i
        # rebuild indices
        old_keys = list(self._indices.keys())
        self._indices.clear()
        for key in old_keys:
            self.create_index(key)

    def create_index(self, column_name: str):
        keyi = self._column_indices[column_name]
        self._indices[column_name] = index = {}
        for row in self.rows:
            assert (row[keyi] not in index), f"Duplicate key {row[keyi]!r} for index {column_name!r}"
            index[row[keyi]] = row

    def add_column(self, col_name: str, col_type: str):
        assert col_type in self.db.converters
        self.columns.append((col_name, col_type))
        self._column_indices[col_name] = len(self.columns) - 1

    def add_row(self, row: list):
        return self.insert_row(len(self.rows), row)
    
    def insert_row(self, i: int, row: list):
        assert len(row) == len(self.columns) - 1
        new_row = [self.next_id]
        self.next_id += 1
        for i in range(len(row)):
            _, col_type = self.columns[i+1]
            new_row.append(self.db.converters[col_type](row[i]))
        self.rows.insert(i, new_row)
        # update indices
        for key, index in self._indices.items():
            keyi = self._column_indices[key]
            assert (new_row[keyi] not in index), f"Duplicate key {new_row[keyi]!r} for index {key!r}"
            index[new_row[keyi]] = new_row
        
    def get_by(self, key: str, value):
        index = self._indices[key]
        row = index.get(value)
        if row is not None:
            return Row(self, row)
        
    def get_by_id(self, id: int):
        assert type(id) is int
        return self.get_by("id", id)

    def objects(self, **queries):
        results = []
        for row in self.rows:
            for k, v in queries.items():
                keyi = self._column_indices[k]
                if row[keyi] != v:
                    break
            else:
                results.append(Row(self, row))
        return results

    def __len__(self):
        return len(self.rows)
    
    def __repr__(self):
        return f"Table(columns={self.columns}, len={len(self)})"
    
    def __getitem__(self, i: int):
        return Row(self, self.rows[i])
    
    def to_dict(self):
        return {
            "name": self.name,
            "columns": self.columns,
            "rows": self.rows,
            "next_id": self.next_id
        }
    
    @staticmethod
    def from_dict(db: Database, data: dict):
        table = Table(db, data["name"])
        table.columns = [tuple(column) for column in data["columns"]]
        table.rows = data["rows"]
        table.next_id = data["next_id"]
        table.rebuild_indices()
        return table
    

class Row:
    def __init__(self, table: Table, row: list):
        self.table = table
        self.row = row

    @property
    def id(self):
        return self.row[0]

    def __getitem__(self, key: int | str):
        if isinstance(key, str):
            key = self.table._column_indices[key]
        item = self.row[key]
        # if item is dbref
        if self.table.columns[key][1] == "dbref":
            item = self.table.db.dereference(item)
        return item
    
    def __repr__(self):
        cpnts = [f"{self.table.columns[i][0]}={self.row[i]!r}" for i in range(len(self.row))]
        return f"Row({', '.join(cpnts)})"

    @property
    def dbref(self):
        return f"{self.table.name}:{self.id}"


db = Database()
t = db.create_table("person")
t.add_column("name", "string")
t.add_column("age", "int")

t2 = db.create_table("address")
t2.add_column("who", "dbref")
t2.add_column("street", "string")

t.add_row(["Alice", 25])
t.add_row(["Bob", 30])
t.add_row(["Charlie", 35])

t2.add_row([t[0].dbref, "123 Main St"])

t.create_index("name")

print(t._column_indices)
print(t._indices)

print(db.save())


print(t.objects(name='Alice'))
print(t.get_by("name", "Alice"))
print(t2[0]["who"])