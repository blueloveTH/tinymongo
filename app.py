import streamlit as st
import sys
import pandas as pd
import pickle as pkl
import base64

from tinymongo.columns import COLUMN_TYPES
from tinymongo.db import Database, Table
from tinymongo.style import setup_style
from tinymongo.translation import Translation, TranslationCN
from tinymongo.exporters.python import export as export_python

sidebar = st.sidebar

st.set_page_config(layout="wide")
setup_style()

tr = TranslationCN

DB_KEY = 'database/db'
DRAFT_DF_KEY = 'tmp/draft_df'
IMPORT_DB_KEY = 'tmp/import_db'
COPY_REF_KEY = 'tmp/copy_ref'

# force pickle to work
__main__ = sys.modules['__main__']
__main__.Database = Database
__main__.Table = Table
                       
if DB_KEY not in st.session_state:
    st.session_state[DB_KEY] = Database('db')
db: Database = st.session_state[DB_KEY]

def save_draft_df():
    if DRAFT_DF_KEY in st.session_state and db.current_table:
        db.current_table.df = st.session_state.pop(DRAFT_DF_KEY)

if db.tables:
    for table_name in db.tables:
        if sidebar.button(table_name, key=table_name, use_container_width=True):
            save_draft_df()
            db.set_current_table(table_name)
            st.rerun()

sidebar.subheader(tr.CreateTable)
_0, _1 = sidebar.columns(2)
table_name = _0.text_input("Table name", label_visibility="collapsed")
if _1.button(tr.CreateTable):
    if table_name:
        if not table_name.isidentifier():
            sidebar.error(tr.InvalidName)
        else:
            if table_name in db.tables:
                sidebar.error(tr.TableExists.format(table_name))
            else:
                db.create_table(table_name)
                save_draft_df()
                db.set_current_table(table_name)
                st.rerun()

sub_cols = st.columns([1, 1, 1, 1.2, 1, 1])

# add row
if sub_cols[0].button(tr.InsertRow) and db.current_table:
    assert len(db.current_table.df.columns) > 0
    save_draft_df()
    df = db.current_table.df
    data = {col: db.current_table.get_column_type(col).default for col in df.columns}
    next_id = db.current_table.next_id()
    df.loc[next_id] = data

    selected_idx = df.index[df['?'] == True]
    if len(selected_idx) > 0:
        new_index = df.index.to_list()
        new_index.pop()     # remove last index
        insert_pos = new_index.index(selected_idx[-1]) + 1
        new_index.insert(new_index.index(selected_idx[-1])+1, next_id)
        df = db.current_table.df = df.loc[new_index]
        df.loc[:, '?'] = False
        df.loc[next_id, '?'] = True
# delete row
if sub_cols[1].button(tr.DeleteRow) and db.current_table:
    save_draft_df()
    df = db.current_table.df
    selected_idx = df.index[df['?'] == True]
    if len(selected_idx) > 0:
        df.drop(selected_idx, inplace=True)

# copy dbref
if sub_cols[2].button(tr.CopyDBRef) and db.current_table:
    save_draft_df()
    df = db.current_table.df
    selected_idx = df.index[df['?'] == True]
    if len(selected_idx) == 1:
        st.session_state[COPY_REF_KEY] = f'^{db.current_table.name}:{selected_idx[-1]}'

if COPY_REF_KEY in st.session_state:
    st.code(st.session_state[COPY_REF_KEY], language='text')
    del st.session_state[COPY_REF_KEY]

if sub_cols[4].button(tr.ImportDB):
    st.session_state[IMPORT_DB_KEY] = db

if IMPORT_DB_KEY in st.session_state:
    uploaded_file = st.file_uploader(tr.ChooseFile, type="py")
    if uploaded_file is not None:
        db_src = uploaded_file.getvalue().decode()
        metadata, *src = db_src.split('\n')
        metadata = metadata.removeprefix('# ')
        new_db: Database = pkl.loads(base64.b64decode(metadata))
        st.session_state.clear()
        st.session_state[DB_KEY] = new_db
        st.rerun()

if sub_cols[5].button(tr.ExportDB):
    save_draft_df()
    exported = export_python(db)
    size_in_kb = int(len(exported) / 1024)
    st.download_button(label=f"Download ({size_in_kb} KB)", data=export_python(db), file_name='db.py')

# add column
sidebar.subheader(tr.CreateColumn)
_0, _1 = sidebar.columns(2)
col_name = _0.text_input("##Column name", label_visibility="collapsed")
col_type_name = _1.selectbox("##Column type", list(COLUMN_TYPES.keys()), label_visibility="collapsed")

_0, _1 = sidebar.columns(2)
if _0.button(tr.CreateColumn):
    if col_name:
        if not col_name.isidentifier():
            sidebar.error(tr.InvalidName)
        else:
            save_draft_df()
            df = db.current_table.df
            if col_name in df.columns:
                sidebar.error(tr.ColumnExists.format(col_name))
            else:
                col_type = COLUMN_TYPES[col_type_name]
                df[col_name] = [col_type.default] * len(df)
                df[col_name] = df[col_name].astype(col_type.dtype)
                db.current_table.column_types[col_name] = col_type_name

if _1.button(tr.DeleteColumn):
    save_draft_df()
    df = db.current_table.df
    if col_name and col_name in df.columns:
        df.drop(col_name, axis=1, inplace=True)
        db.current_table.column_types.pop(col_name)

if db.current_table is None:
    st.info(tr.WelcomeMessage)
    st.stop()

df: pd.DataFrame = db.current_table.df

column_config = {"": st.column_config.TextColumn("id", disabled=True)}
for col_name in df.columns:
    col_type = db.current_table.get_column_type(col_name)
    if col_type.name == 'dbref':
        col_label = f"{col_name} ({col_type.name})"
    else:
        col_label = col_name
    column_config[col_name] = col_type.get_config(col_label)

st.session_state[DRAFT_DF_KEY] = st.data_editor(
    df,
    column_config=column_config,
    key='current_table',
    # height=int((len(df)+1) * 35.0 + 5.0),
    # height=int((min(14, len(df))+1) * 35.0 + 5.0),
)