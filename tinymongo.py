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
        if isinstance(value, Row):
            row = value
        elif isinstance(value, str):
            row = self.dereference(value)
            assert row is not None
        else:
            raise TypeError(f"invalid dbref {value!r}")
        return row.dbref
    def _to_list(self, value: str):
        if isinstance(value, list):
            return value
        ret = json.loads(value)
        assert isinstance(ret, list)
        return ret
    def _to_dict(self, value: str):
        if isinstance(value, dict):
            return value
        ret = json.loads(value)
        assert isinstance(ret, dict)
        return ret
    
    def check_integrity(self, logs=None) -> bool:
        if logs is None:
            logs = []
        for table in self.tables.values():
            dbref_cols = [col for col in table.columns if col.type == "dbref"]
            dbref_cols = [table._column_indices[col.name] for col in dbref_cols]
            for row in table.rows:
                for i in dbref_cols:
                    value = row.data[i]
                    if value is not None:
                        target_row = self.dereference(value)
                        if target_row is None:
                            logs.append(f"{table.name}: invalid dbref {value!r} in {row!r} was removed")
                            row[i] = None
        return len(logs) == 0

    def add_table(self, name: str):
        assert name.isidentifier()
        t = Table(self, name)
        self.tables[name] = t
        return t

    def __getitem__(self, name: str):
        return self.tables[name]
    
    def dereference(self, ref: str):
        table_name, row_id = ref.split(":")
        return self.tables[table_name].with_id(int(row_id))
    
    def __len__(self):
        return len(self.tables)
    
    def save(self):
        return json.dumps([t._to_json_object() for t in self.tables.values()])
    
    @staticmethod
    def load(data: str):
        db = Database()
        for table_data in json.loads(data):
            table = Table._from_json_object(db, table_data)
            db.tables[table.name] = table
        return db

class Table:
    db: Database
    name: str
    columns: list['Column']
    rows: list['Row']
    next_id: int

    _column_indices: dict[str, int]
    _indices: dict[int, 'Row']

    def __init__(self, db: Database, name: str):
        self.db = db
        self.name = name
        self.columns = [Column(self, "id", "int")]
        self.rows = []
        self.next_id = 1000

        self._column_indices = {"id": 0}
        self._indices = {}

    def delete(self):
        for row in self.rows:
            row.delete()
        del self.db.tables[self.name]

    def rebuild_column_index(self):
        self._column_indices = {col.name: i for i, col in enumerate(self.columns)}

    def rebuild_indices(self):
        self._indices = {row.id: row for row in self.rows}

    def add_column(self, col_name: str, col_type: str):
        return self.insert_column(len(self.columns), col_name, col_type)

    def add_row(self, data: list[str] = None):
        return self.insert_row(len(self.rows), data)
    
    def insert_column(self, i: int, col_name: str, col_type: str):
        assert col_name.isidentifier()
        assert col_name not in self._column_indices
        assert col_type in self.db.converters
        self.columns.insert(i, Column(self, col_name, col_type))
        # rebuild column indices
        self._column_indices = {col.name: i for i, col in enumerate(self.columns)}
    
    def insert_row(self, i: int, data: list[str] = None):
        new_row = Row(self, [self.next_id] + [None] * (len(self.columns) - 1))
        self.next_id += 1
        # update indices
        self._indices[new_row.id] = new_row
        self.rows.insert(i, new_row)
        if data is not None:
            assert len(data) == len(self.columns) - 1
            for i in range(len(data)):
                new_row[i+1] = data[i]

    def with_id(self, id: int):
        assert isinstance(id, int)
        return self._indices.get(id)

    def objects(self, **queries):
        results = []
        for row in self.rows:
            for k, v in queries.items():
                keyi = self._column_indices[k]
                if row[keyi] != v:
                    break
            else:
                results.append(row)
        return results

    def __len__(self):
        return len(self.rows)
    
    def __repr__(self):
        return f"Table(columns={self.columns}, len={len(self)})"
    
    def __getitem__(self, i: int):
        return self.rows[i]
    
    def __iter__(self):
        return iter(self.rows)
    
    def _to_json_object(self):
        return {
            "name": self.name,
            "columns": [col._to_json_object() for col in self.columns],
            "rows": [row._to_json_object() for row in self.rows],
            "next_id": self.next_id,
        }
    
    @staticmethod
    def _from_json_object(db: Database, data: dict):
        table = Table(db, data["name"])
        table.columns = [Column._from_json_object(table, col) for col in data["columns"]]
        table.rows = [Row._from_json_object(table, row) for row in data["rows"]]
        table.next_id = data["next_id"]
        table.rebuild_column_index()
        table.rebuild_indices()
        return table

class Column:
    def __init__(self, table: Table, name: str, type: str):
        self.table = table
        self.name = name
        self.type = type

    def __repr__(self):
        return f"Column({self.name!r}, {self.type!r})"
    
    def delete(self):
        if self.name == "id":
            raise ValueError("cannot delete id column")
        keyi = self.table._column_indices[self.name]
        del self.table.columns[keyi]
        self.table.rebuild_column_index()
    
    def _to_json_object(self):
        return {"name": self.name, "type": self.type}
    
    @staticmethod
    def _from_json_object(table: Table, data: dict):
        return Column(table, data["name"], data["type"])

class Row:
    def __init__(self, table: Table, data: list):
        self.table = table
        self.data = data

    @property
    def id(self) -> int:
        return self.data[0]
    
    @property
    def db(self) -> Database:
        return self.table.db

    @property
    def dbref(self):
        return f"{self.table.name}:{self.id}"

    def __getitem__(self, key: int | str):
        if isinstance(key, str):
            key = self.table._column_indices[key]
        item = self.data[key]
        if item is None:
            return None
        # if item is dbref
        if self.table.columns[key].type == "dbref":
            item = self.db.dereference(item)
        return item
    
    def __setitem__(self, key: int | str, value):
        if isinstance(key, str):
            key = self.table._column_indices[key]
        if key == 0:
            raise ValueError("id is read-only")
        # if value is dbref
        col_type = self.table.columns[key].type
        if value is not None:
            value = self.db.converters[col_type](value)
        self.data[key] = value
    
    def __repr__(self):
        cpnts = [f"{self.table.columns[i].name}={self.data[i]!r}" for i in range(len(self.data))]
        return f"Row({', '.join(cpnts)})"
    
    def delete(self):
        self.table.rows.remove(self)
        # remove from indices
        del self.table._indices[self.id]

    def _to_json_object(self):
        return self.data
    
    @staticmethod
    def _from_json_object(table: Table, data: list):
        row = Row(table, data["data"])
        return row


db = Database()
t = db.add_table("person")
t.add_column("name", "string")
t.add_column("age", "int")

t2 = db.add_table("address")
t2.add_column("who", "dbref")
t2.add_column("street", "string")

t.add_row(["Alice", None])
t.add_row(["Bob", 30])
t.add_row(["Charlie", 35])

t2.add_row([t[0], "123 Main St"])

# t2[0]['who'] = None
print(t2[0][1])

print(t._column_indices)
print(t._indices)

logs = []
if not db.check_integrity(logs):
    print(logs)
print(db.save())

print(t.objects(name='Alice'))
print(t2[0]["who"])
