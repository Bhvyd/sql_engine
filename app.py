import streamlit as st
from mini_mysql import MiniMySQL

# Ensure the database engine is only initialized once
if "db_instance" not in st.session_state:
    st.session_state.db_instance = MiniMySQL()

st.set_page_config(page_title="MiniMySQL", layout="centered")
st.title("🛢️ MiniMySQL")

sql_input = st.text_area("Enter SQL command:", height=150)

if st.button("Execute"):
    if sql_input.strip():
        try:
            output = st.session_state.db_instance.execute(sql_input.strip(';'))
            st.text_area("Output:", value=output, height=300)
        except Exception as e:
            st.error(f"Error: {e}")
    else:
        st.warning("Please enter a SQL command.")
