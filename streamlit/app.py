import streamlit as st
import numpy as np
import pandas as pd

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

result.drop(["stop_id", "timestamp", "ct"], axis = 1, inplace = True)

result.set_index("pt", inplace = True, drop = False)

result["hour"] = result.index.hour.fillna(0).astype(np.int16)
result["weekday"] = result.index.day_name()
result["month"] = result.index.month_name()
result["year"] = result.index.year

# * Define sidebars

st.set_page_config(layout="wide")

journey_type = st.sidebar.multiselect("Filter journey types",
            pd.unique(result["f"]),
            default = pd.unique(result["f"]))

filtered_journeys = result[result["f"].isin(journey_type)]

train_type = st.sidebar.multiselect("Filter train types",
            pd.unique(filtered_journeys["c"]),
            default = pd.unique(filtered_journeys["c"]))

filtered_all = result[result["f"].isin(journey_type) & result["c"].isin(train_type)]

train_number = st.sidebar.multiselect("Filter train number",
            pd.unique(filtered_all["n"]))

if train_number:
    filtered_all = result[result["f"].isin(journey_type) & result["c"].isin(train_type) & result["n"].isin(train_number)]
else:
    filtered_all = result[result["f"].isin(journey_type) & result["c"].isin(train_type)]


import datetime
today = datetime.date.today()

date_from = st.sidebar.date_input("Start date", result.index.min())
date_to = st.sidebar.date_input("End date", today)

# * Main page

st.title('My first app')

col1, col2 = st.columns(2)

import plotly.express as px
import plotly.figure_factory as ff

plot_alldays = px.histogram(filtered_all, x = "delay", hover_data = filtered_all.columns)
col1.plotly_chart(plot_alldays)

plot_hours = px.violin(filtered_all, y = "delay", x = "hour", points = "outliers", hover_data = filtered_all.columns)
col2.plotly_chart(plot_hours)

plot_weekdays = px.violin(filtered_all, y = "delay", x = "weekday", points = "outliers", hover_data = filtered_all.columns, box = True)
col1.plotly_chart(plot_weekdays)

plot_months = px.violin(filtered_all, y = "delay", x = "month", points = "outliers", hover_data = filtered_all.columns, box = True)
col2.plotly_chart(plot_months)

plot_years = px.violin(filtered_all, y = "delay", x = "year", points = "outliers", hover_data = filtered_all.columns, box = True)
col1.plotly_chart(plot_years)


filtered_all


#! Zeit filter ermöglichen

#! Verspätunsgcodes reparieren


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