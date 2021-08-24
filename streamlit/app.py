import streamlit as st
import numpy as np
import pandas as pd
import datetime
import plotly.express as px



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

result["minute"] = result.index.minute.fillna(0).astype(np.int16)
result["hour"] = result.index.hour.fillna(0).astype(np.int16)
result["day"] = result.index.day.fillna(0).astype(np.int16)
result["weekday"] = result.index.day_name()
result["month"] = result.index.month_name()
result["year"] = result.index.strftime("%Y").astype(str)

# * Define sidebars

st.set_page_config(layout="wide")

st.sidebar.header("Filter")

# Datum

today = datetime.date.today()

date_from = st.sidebar.date_input("Start date", result.index.min())
date_to = st.sidebar.date_input("End date", today)

filtered_days = result.loc[date_from.strftime("%Y-%m-%d"):date_to.strftime("%Y-%m-%d")]

# Journey type
# "f": "string", # filter flags. Siehe 1.2.26. D = external, F = long distance, N = regional, S = SBahn

# journey_type = st.sidebar.multiselect("Filter journey types",
#             pd.unique(result["f"]),
#             default = pd.unique(result["f"]))

# filtered_journeys = filtered_days[filtered_days["f"].isin(journey_type)]


# Train type
train_type = st.sidebar.multiselect("Filter train types",
            pd.unique(filtered_days["c"]),
            default = pd.unique(filtered_days["c"]))

filtered_type = filtered_days[filtered_days["c"].isin(train_type)]

# Train number
train_number = st.sidebar.multiselect("Filter train number",
            pd.unique(filtered_type["n"]))

if train_number:
    filtered_all = filtered_type[filtered_type["n"].isin(train_number)]
else:
    filtered_all = filtered_type


# * Main page

st.title("Verspätungsmonitor München-Pasing")

col1, col2 = st.columns(2)

plot_labels = {"delay": "delay (minutes)"}

plot_alldays = px.histogram(filtered_all, x = "delay", hover_data = filtered_all.columns, title = "Verspätungen aller Züge", labels = plot_labels)
col1.plotly_chart(plot_alldays)

plot_hours = px.violin(filtered_all, y = "delay", x = "hour", points = "outliers", hover_data = filtered_all.columns, color = "hour", title = "Verspätungen je nach Stunde", labels = plot_labels)
col2.plotly_chart(plot_hours)

plot_weekdays = px.violin(filtered_all, y = "delay", x = "weekday", points = "outliers", hover_data = filtered_all.columns, box = True, color = "weekday", title = "Verspätung je nach Wochentag", labels = plot_labels)
col1.plotly_chart(plot_weekdays)

plot_months = px.violin(filtered_all, y = "delay", x = "month", points = "outliers", hover_data = filtered_all.columns, box = True, color = "month", title = "Verspätung je nach Monat", labels = plot_labels)
col2.plotly_chart(plot_months)

plot_years = px.violin(filtered_all, y = "delay", x = "year", points = "outliers", hover_data = filtered_all.columns, box = True, color = "year", title = "Verspätung je nach Jahr", labels = plot_labels)
col1.plotly_chart(plot_years)

col2.write("")
col2.write("")
col2.markdown("Alle Daten")

col2.write("")

col2.dataframe(filtered_all.reset_index(drop = True)[["c", "n", "weekday", "day", "month", "year", "hour", "minute", "delay"]])

#! Code sicherer machen für Veröffentlichung

#! Verspätunsgcodes reparieren

#! Alternative ausprobieren: Heatmaps mit %

#! Prüfen, ob alle Züge + Verspätungen richtig erfasst werden

#! Airflow-Logs müssen periodisch gelöscht werden, sonst nehmen sie wirklich viel Speicherplatz weg

#! Wenn man den join-dag manuell auslöst, dann geht gar nichts mehr richtig


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