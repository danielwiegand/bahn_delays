import streamlit as st
import numpy as np
import pandas as pd

st.title('My first app')

HOST = "postgres_streams"
PORT = "5432"
USER = "postgres"
PASSWORD = "postgres"
DB = "bahn"

conn_string = f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}' 

from sqlalchemy import create_engine
conn = create_engine(conn_string, echo = True).connect()

query = """SELECT * FROM delays;"""

result = pd.read_sql(query, con = conn)


# * Data wrangling on results df

result["n"] = result["n"].astype("int64")
result["delay"] = result["delay"].fillna(0).astype(np.int64)

result.drop(["stop_id", "timestamp"], axis = 1, inplace = True)

result.set_index("pt", inplace = True)

result["hour"] = result.index.hour.fillna(0).astype(np.int16)


# * Define sidebars

journey_type = st.sidebar.multiselect("Filter journey types",
            pd.unique(result["f"]),
            default = pd.unique(result["f"]))

filtered_journeys = result[result["f"].isin(journey_type)]

train_type = st.sidebar.multiselect("Filter train types",
            pd.unique(filtered_journeys["c"]),
            default = pd.unique(filtered_journeys["c"]))

filtered_trains = result[result["f"].isin(journey_type) & result["c"].isin(train_type)]

train_number = st.sidebar.multiselect("Filter train number",
            pd.unique(filtered_trains["n"]),
            default = sorted(pd.unique(filtered_trains["n"])))

filtered_all = result[result["f"].isin(journey_type) & result["c"].isin(train_type) & result["n"].isin(train_number)]


import datetime
today = datetime.date.today()

date_from = st.sidebar.date_input("Start date", result.index.min())
date_to = st.sidebar.date_input("End date", today)


import plotly.express as px
import plotly.figure_factory as ff


hist = px.histogram(filtered_all, x = "delay", hover_data = filtered_all.columns)
st.plotly_chart(hist)

violin = px.violin(filtered_all, y = "delay", x = "hour", box = True, points = "all", hover_data = filtered_all.columns)
st.plotly_chart(violin)

# import seaborn as sns
# time_chart = sns.catplot(data = filtered_all, x = "hour", y = "delay", kind = "box")
# st.pyplot(time_chart)


filtered_all



# * CSS styles

# Define sidebar width
st.markdown(
    f'''
        <style>
            .css-1lcbmhc .css-hby737 {{
                width: 375px;
            }}
        </style>
    ''',
    unsafe_allow_html = True
)

st.markdown(
    f'''
        <style>
            .element-container:nth-child(3) .st-bv {{
                height: 200px;
                overflow: scroll;
            }}
        </style>
    ''',
    unsafe_allow_html = True
)


# st.button("Update", key=None, help=None, on_click=None, args=None, kwargs=None)