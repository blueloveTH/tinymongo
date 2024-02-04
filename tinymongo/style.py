import streamlit as st

def setup_style():
    st.markdown("""
    <style>  
        div.block-container {
            padding-top: 2rem;          // 改变总体的padding
        }
        div[data-testid="stSidebarUserContent"] {
            padding-top: 2rem;          // 改变sidebar的padding
        }
    </style>
    """, unsafe_allow_html=True)

    st.markdown("""
    <style>
        section.main {
            div[data-testid="column"] {
                width: fit-content !important;
                flex: unset;
            }
            div[data-testid="column"] * {
                width: fit-content !important;
            }
        }
    </style>
    """, unsafe_allow_html=True)