import streamlit as st

def setup_style():
    st.markdown("""
    <style>
        .st-emotion-cache-16txtl3 {
            padding: 3rem 1.5rem;       // 改变sidebar的padding
        }
        .st-emotion-cache-phzz4j {
            gap: 0.4rem;                // 改变sidebar按钮的间距
        }     
        div.block-container {
            padding-top: 2rem;          // 改变main的padding
        }
    </style>
    """, unsafe_allow_html=True)

# st.markdown("""
#             <style>
#                 div[data-testid="column"] {
#                     width: fit-content !important;
#                     flex: unset;
#                 }
#                 div[data-testid="column"] * {
#                     width: fit-content !important;
#                 }
#             </style>
#             """, unsafe_allow_html=True)