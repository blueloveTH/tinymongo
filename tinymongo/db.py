import json

def _to_string(value: str):
    return value

def _to_int(value: str):
    return int(value)

def _to_float(value: str):
    return float(value)

def _to_bool(value: str):
    return bool(value)

def _to_reference(value: str):
    return value

class Database:
    tables: dict[str, 'Table']

    def __init__(self):
        self.tables = {}

    def create_table(self, name: str):
        t = Table()
        self.tables[name] = t
        return t

    def __getitem__(self, name: str):
        return self.tables[name]
    
    def __len__(self):
        return len(self.tables)
    
    def save(self):
        return json.dumps({name: table.to_dict() for name, table in self.tables.items()})
    
    @staticmethod
    def load(data: str):
        db = Database()
        for name, table_data in json.loads(data).items():
            db.tables[name] = Table.from_dict(table_data)
        return db


class Row:
    pass


class Table:
    columns: list[tuple[str, str]]
    rows: list

    def __init__(self):
        self.columns = [("id", "int")]
        self.rows = []
        self.next_id = 100000

        self._column_indices = {"id": 0}
        self._indices = {"id": {}}

    def rebuild_indices(self):
        self._column_indices.clear()
        for i in range(len(self.columns)):
            self._column_indices[self.columns[i][0]] = i
        for key in self._indices.keys():
            keyi = self._column_indices[key]
            self._indices[key] = {row[keyi]: row for row in self.rows}

    def _convert(self, value, column_type: str):
        f = f"_to_{column_type}"
        return globals()[f](value)

    def add_column(self, column_name: str, column_type: str):
        assert column_type in ("string", "int", "float", "bool", "reference")
        self.columns.append((column_name, column_type))
        self._column_indices[column_name] = len(self.columns) - 1

    def add_row(self, row):
        assert len(row) == len(self.columns) - 1
        new_row = [self.next_id]
        self.next_id += 1
        for i in range(len(row)):
            _, col_type = self.columns[i+1]
            new_row.append(self._convert(row[i], col_type))
        self.rows.append(new_row)
        # update indices
        for key, index in self._indices.items():
            keyi = self._column_indices[key]
            assert new_row[keyi] not in index
            index[new_row[keyi]] = new_row

    def __len__(self):
        return len(self.rows)
    
    def __repr__(self):
        return f"Table(columns={self.columns}, len={len(self)})"
    
    def to_dict(self):
        return {
            "columns": self.columns,
            "rows": self.rows,
            "next_id": self.next_id
        }
    
    @staticmethod
    def from_dict(d: dict):
        table = Table()
        table.columns = [tuple(column) for column in d["columns"]]
        table.rows = d["rows"]
        table.next_id = d["next_id"]
        table.rebuild_indices()
        return table

db = Database()
t = db.create_table("person")
t.add_column("name", "string")
t.add_column("age", "int")

t.add_row(["Alice", 25])
t.add_row(["Bob", 30])
t.add_row(["Charlie", 35])

print(t._column_indices)
print(t._indices)

print(db.save())
