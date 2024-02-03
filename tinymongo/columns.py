import streamlit as st

class ColumnType:
    def get_config(self, label: str):
        raise NotImplementedError
    
    @property
    def default(self):
        raise NotImplementedError
    
    @property
    def dtype(self):
        raise NotImplementedError
    
    @property
    def name(self):
        cls_name = self.__class__.__name__
        cls_name = cls_name.removeprefix('ColumnType')
        return cls_name.lower()

class ColumnTypeStr(ColumnType):
    default = ''
    dtype = 'object'
    def get_config(self, label: str):
        return st.column_config.TextColumn(label, default='')
    
class ColumnTypeInt(ColumnType):
    default = 0
    dtype = 'int64'
    def get_config(self, label: str):
        return st.column_config.NumberColumn(label, default=0, step=1, min_value=-10000000, max_value=10000000)
    
class ColumnTypeFloat(ColumnType):
    default = 0.0
    dtype = 'float64'
    def get_config(self, label: str):
        return st.column_config.NumberColumn(label, default=0.0, min_value=-100000, max_value=100000)
    
class ColumnTypeBool(ColumnType):
    default = False
    dtype = 'bool'
    def get_config(self, label: str):
        return st.column_config.CheckboxColumn(label, default=False)
    
class ColumnTypeDbref(ColumnType):
    default = ''
    dtype = 'object'
    def get_config(self, label: str):
        return st.column_config.TextColumn(label, default='')
    
COLUMN_TYPES: dict[str, ColumnType] = {
    'str': ColumnTypeStr(),
    'int': ColumnTypeInt(),
    'float': ColumnTypeFloat(),
    'bool': ColumnTypeBool(),
    'dbref': ColumnTypeDbref(),
}