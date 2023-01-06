import streamlit as st
import snowflake.connector
import pandas as pd

cnn = snowflake.connector.connect(
    user = "elim0412",
    password = "justType1234",
    account = "wqtoeej-oh18991",
    warehouse = "compute_wh",
    database = "summit_hol",
    schema = "public"
)

cs = cnn.cursor()
sql = "select * from employee"
cs.execute(sql)
df = cs.fetch_pandas_all()
cs.close()
cnn.close()

# print(df.head())

st.dataframe(df)