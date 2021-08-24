import json
import logging
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import pymongo
import pytz
import requests
import xmltodict
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer
from sqlalchemy import create_engine

load_dotenv() # load env variables from .env

HEADERS = {"Authorization": f"Bearer {os.getenv('BEARER')}",
           "Accept": "application/xml"}

now = datetime.now(tz = pytz.timezone("Europe/Berlin"))
in_one_hour = now + timedelta(hours = 1)

request_hour = in_one_hour.strftime("%H")
request_date = in_one_hour.strftime("%y%m%d")

eva = 8004158


# XML TO JSON
def convert_to_json(txt):
  return xmltodict.parse(txt)


# * SEND TO KAFKA #############################

def prepare_url(topic, eva, date, hour):
    if topic == "timetable":
        url = f"https://api.deutschebahn.com/timetables/v1/plan/{eva}/{date}/{hour}"
    elif topic == "changes":
        url = f"https://api.deutschebahn.com/timetables/v1/rchg/{eva}"
    return url


def request_and_send(url, topic):
    
    producer = KafkaProducer(bootstrap_servers = "kafka-server:9092",
                         value_serializer = lambda m: json.dumps(m).encode('utf-8')
                         )

    try:
        r = requests.get(url, headers = HEADERS)
        if r.status_code == 200:
            tt = convert_to_json(r.text)
            for train_stop in tt["timetable"]["s"]:
                producer.send(topic = topic,
                            value = train_stop)
                time.sleep(1)
        else:
            logging.critical(f"ERROR: Could not fetch data from Deutsche Bahn: {r.content}")
    except requests.exceptions.RequestException as e: 
        logging.critical(f"ERROR when connecting to the Bahn API: {e}")


def send_to_kafka(topic):
    url = prepare_url(topic, eva, request_date, request_hour)    
    print(f"URL to fetch: {url}")
    request_and_send(url, topic)
    
    
# * SEND TO MONGODB #############################

def connect_to_mongo():
    # create a connection
    client = pymongo.MongoClient(host = 'mongo', port = 27017)
    # connect to a database
    db = client.bahn
    return db


def send_to_mongo(topic):

    consumer = KafkaConsumer(topic,
                            bootstrap_servers = "kafka-server:9092",
                            auto_offset_reset = 'earliest',
                            enable_auto_commit = True, # If True , the consumer’s offset will be periodically committed in the background.
                            value_deserializer = lambda m: json.loads(m.decode('utf-8')),
                            consumer_timeout_ms = 1000, 
                            group_id = 'my-group-1')
    
    db = connect_to_mongo()

    for entry in consumer:
        
        value = entry.value
        
        if topic == "timetable":
            value["timestamp"] = int(in_one_hour.strftime("%y%m%d%H"))
        
        try:
            db[topic].update(
                {"@id": value["@id"]},
                value,
                upsert = True
                )
        
        except TypeError:
            print("Type error occured while entry was written to mongoDB!")
            pass
    
    consumer.close() # close consumer if no message for 1 second (consumer_timeout_ms)


# * JOIN TIMETABLE AND CHANGES #############################

def connect_to_postgres():
    HOST = "postgres_streams"
    PORT = 5432
    USER = "postgres"
    PASSWORD = "postgres"
    DB = "bahn"

    conn_string = f"postgres://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}"
    conn = create_engine(conn_string, echo = True).connect()
    
    return conn

def fetch_from_postgres(connection, table_name):
  query = f"SELECT * FROM {table_name};"
  result = connection.execute(query)
  df = pd.DataFrame(result, columns = result.keys())
  return df

def convert_to_datetime(column):
    out = column.str.replace(r"(\w\w)(\w\w)(\w\w)(\w\w\w\w)", r"\1-\2-\3 \4").apply(pd.to_datetime, yearfirst = True)
    return out

def upsert_into_postgres(connection, df):
    create_table_query = """CREATE TABLE IF NOT EXISTS delays (
        stop_id TEXT PRIMARY KEY,
        c TEXT,
        f TEXT,
        n TEXT,
        pt TIMESTAMP WITHOUT TIME ZONE,
        ct TIMESTAMP WITHOUT TIME ZONE,
        code TEXT,
        timestamp TIMESTAMP WITHOUT TIME ZONE,
        delay DOUBLE PRECISION
        );"""
    connection.execute(create_table_query)

    df.to_sql(name = "temp_table", con = connection, 
              if_exists = "replace", index = False)

    colnames = list(df.columns)
    conflict_subquery = ",".join([f"{col} = EXCLUDED.{col}" for col in colnames])
    
    query = f"""INSERT INTO delays (SELECT * FROM temp_table) ON CONFLICT (stop_id) DO UPDATE SET {conflict_subquery};"""
    connection.execute(query)


def join_timetable_changes():
    conn = connect_to_postgres()
    
    timetable_df = fetch_from_postgres(conn, "timetable")
    changes_df = fetch_from_postgres(conn, "changes")
    
    changes_df = changes_df.dropna(subset = ["ct"]).sort_values(by = "timestamp", ascending = False).groupby("stop_id").head(1)
    
    result = pd.merge(timetable_df.drop("timestamp", axis = 1), changes_df, on = "stop_id", how = "left")
    
    result["ct"].fillna(result["pt"], inplace = True)
    
    result["pt"] = convert_to_datetime(result["pt"])
    result["ct"] = convert_to_datetime(result["ct"])
    
    result["delay"] = ((result["ct"] - result["pt"]).dt.total_seconds() / 60)
    
    upsert_into_postgres(conn, result)
    
    
# * EMPTY INTERMEDIARY DATABASES #############################

def delete_old_entries():
    
    conn = connect_to_postgres()
    
    now = datetime.now(tz = pytz.timezone("Europe/Berlin"))
    one_day_ago = (now + timedelta(hours = -24)).astimezone(pytz.utc)
    
    for table_name in ["timetable", "changes"]:
        query = f"DELETE FROM {table_name} WHERE timestamp < TIMESTAMP '{one_day_ago}'"
        conn.execute(query)
