import streamlit as st
import json
import pandas as pd
from st_aggrid import AgGrid, GridUpdateMode
from st_aggrid.grid_options_builder import GridOptionsBuilder
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

def execute_query(conn, df_row, table_name):
    if not df_row.empty:
        # conn.cursor().execute(
        #     "CREATE OR REPLACE TABLE "
        #     f"{table_name}(COUNTRY string, CAPITAL string, TYPE string)"
        # )
        write_pandas(
            conn=conn,
            df=df_row,
            table_name=table_name,
            database="SUMMIT_HOL",
            schema="PUBLIC",
            quote_identifiers=False,
        )

# Initialize connection.
# Uses st.experimental_singleton to only run once.
@st.experimental_singleton
def init_connection():
    return snowflake.connector.connect(**st.secrets["snowflake"])


# Set page context
st.set_page_config(page_title="DataEdit Demo", page_icon="📊")

# ---- SIDEBAR ----
#Add header and a subheader
st.title("Data Editing Demo")

with snowflake.connector.connect(**st.secrets["snowflake"]) as cnn:
    employeeDf = cnn.cursor().execute("select * from employee").fetch_pandas_all()

# main entry form
with st.expander("Please expand to add new entry!", expanded=False):
    with st.form("my_form"):
        st.write("### Enter employee details")

        name_val = st.text_input("Name")
        age_val = st.slider("Age",18,99,30)

        experience_val = st.slider("Years of Experience",1,20,3)

        job_val  = st.selectbox(
        'Job title',
        ('Engineer', 'Marketing Manager', 'Sales Director', 'Executive'))

        salary_val = st.number_input("Salary", step= 1000, value=50000)

        insider_val = st.checkbox("Insider?")
        # Every form must have a submit button.
        submitted = st.form_submit_button("Save Employee")
        if submitted:
            # creates a new df and appends to the DB
            st.write("name:", name_val, "| age:", age_val, "| experience: ", experience_val, "| job:", job_val, "| salary: ", salary_val, "| insider:", insider_val)
            # newEmployeeDf=session.createDataFrame([Row(employee_id=str(uuid.uuid4()) ,
            newEmployeeDf = pd.DataFrame({"name": name_val, "age": age_val, "experience": experience_val, "job": job_val, "salary": salary_val, "insider": insider_val}, index=[0])
            execute_query(snowflake.connector.connect(**st.secrets["snowflake"]), newEmployeeDf, "employee")

# Main Data grid
st.write("### Employees database:")
st.info("💡 Hold the `Shift` (⇧) key to select multiple rows at once.")
# df = employeeDf.to_pandas()
df = employeeDf
gd = GridOptionsBuilder.from_dataframe(df)
gd.configure_pagination(enabled=True, paginationAutoPageSize=False, paginationPageSize=10)
gd.configure_default_column(editable=True, groupable=True)
gd.configure_selection(selection_mode="multiple", use_checkbox=True)
gridoptions = gd.build()
grid_table = AgGrid(
    df,
    gridOptions=gridoptions,
    update_mode=GridUpdateMode.SELECTION_CHANGED | GridUpdateMode.VALUE_CHANGED,
    theme="balham",
)

# Selected rows grid
sel_row = grid_table["selected_rows"]
st.subheader("Selected rows:")
df_sel_row = pd.DataFrame(sel_row)
if not df_sel_row.empty:
    if st.button("Delete Selection"):
        st.success('Rows deleted')
    # st.write(df_sel_row)
    st.write(df_sel_row.loc[:, df_sel_row.columns != df_sel_row.columns[0]])

# Modified rows grid
edit_row = grid_table["data"]
st.subheader("Edited rows:")
df_edit_row = pd.DataFrame(edit_row)
# df_orig_row = employeeDf.to_pandas()
df_orig_row = employeeDf
df_orig_row['status'] = "Basic"
df_edit_row['status'] = "Edited"
cols = df_orig_row.columns[:-1].tolist()
df_change = pd.concat([df_orig_row,df_edit_row]).drop_duplicates(subset=cols, keep=False)
if not df_change.empty:
    df_db = df_change[df_change['status'] == 'Edited']
    df_db = df_db[cols]
    df_db['updated_user'] = "CA01"
    table_name = "employee_changes"
    run_query = st.button(
        "Save Changes to DB", on_click=execute_query, args=(init_connection(), df_db, table_name)
    )
    if run_query:
        st.success(
            f"✔️ Changes saved to the `{table_name}` table in snowflake database."
        )
        st.snow()
    # if st.button("Save Changes"):
    #     st.success('Rows saved')

    st.write(df_change.sort_values(by=['NAME','status']))
    
