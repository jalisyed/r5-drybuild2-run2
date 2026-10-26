"""Ask the data: a prompt library over the retail gold tables. Streamlit on Databricks Apps.
Presenter-driven: pick a persona, pick a question, run its pre-validated SQL, read the 'why it matters' line.
NOT yet tested on Databricks Free Edition. Verify (F-checks in the setup guide) before relying on it."""
import json, os
import pandas as pd
import streamlit as st
from databricks.sdk import WorkspaceClient

st.set_page_config(page_title="Ask the data", layout="wide")
LIB = json.load(open(os.path.join(os.path.dirname(__file__), "prompts.json")))
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "")

@st.cache_resource
def client(): return WorkspaceClient()

def run(sql: str) -> pd.DataFrame:
    w = client()
    r = w.statement_execution.execute_statement(statement=sql, warehouse_id=WAREHOUSE_ID, wait_timeout="30s")
    if r.status.state.value != "SUCCEEDED": raise RuntimeError(str(r.status.error))
    cols = [c.name for c in r.manifest.schema.columns]
    return pd.DataFrame(r.result.data_array or [], columns=cols).apply(pd.to_numeric, errors="ignore")

st.title("Ask the data")
st.caption("Synthetic retail data. Every question below runs a reviewed query.")
personas = ["All"] + sorted({p["persona"] for p in LIB})
who = st.sidebar.radio("Who is asking?", personas)
items = [p for p in LIB if who in ("All", p["persona"])]
pick = st.selectbox("Pick a question", items, format_func=lambda p: f"{p['id']}  {p['question']}")
st.markdown(f"**Why it matters:** {pick['why']}")
if st.button("Run", type="primary"):
    try:
        df = run(pick["sql"]); st.dataframe(df, use_container_width=True)
        if df.shape[1] == 2 and pd.api.types.is_numeric_dtype(df.iloc[:, 1]): st.bar_chart(df.set_index(df.columns[0]))
    except Exception as e:
        st.error(f"Query failed: {e}")
with st.expander("Show the SQL"): st.code(pick["sql"], language="sql")
