import json
import logging
import time
from datetime import datetime, timedelta

import pymongo
import pytz
import requests
import xmltodict
from kafka import KafkaConsumer, KafkaProducer
from sqlalchemy import create_engine

HEADERS = {"Authorization": "Bearer c2717c0f768243e30011b8b3104f6d3d",
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

# def connect_to_database():
#     #! Noch ändern, wenn in Docker
#     HOST = 'localhost'
#     PORT = '5555'
#     USER = "postgres"
#     PASSWORD = "postgres"
#     DB = 'bahn'

#     conn_string = f'postgres://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}' 
#     conn = create_engine(conn_string, echo = True).connect()
    
#     return conn

# conn = connect_to_database()
# query = """SELECT DISTINCT * FROM timetable LEFT JOIN (SELECT DISTINCT * FROM changes) ON timetable.stop_id = changes.stop_id;"""

# import pandas as pd
# joined_data = conn.execute(query)
# joined_data_df = pd.DataFrame(joined_data)


# def join_timetable_changes(conn):