import json
import time
from datetime import date, datetime

import pymongo
import requests
import xmltodict
from kafka import KafkaConsumer, KafkaProducer
import logging

HEADERS = {"Authorization": "Bearer c2717c0f768243e30011b8b3104f6d3d",
           "Accept": "application/xml"}

today = date.today()
today_date = int(today.strftime("%y%m%d"))

now = datetime.now()
hour = int(now.strftime("%H")) + 1

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

    r = requests.get(url, headers = HEADERS)
    if r.status_code == 200:
        tt = convert_to_json(r.text)
        for train_stop in tt["timetable"]["s"]:
            producer.send(topic = topic,
                          value = train_stop)
            time.sleep(1)
    else:
        print(f"ERROR: Could not fetch data from Deutsche Bahn: {r.content}")


def send_to_kafka(topic):
    url = prepare_url(topic, eva, today_date, hour)
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
    consumer.close() # close consumer if no message for 1 second (consumer_timeout_ms)
    
    db = connect_to_mongo()

    for entry in consumer:
        
        try:
            db[topic].update(
                {"@id": entry.value["@id"]},
                entry.value,
                upsert = True
                )
        except TypeError:
            print("Type error occured while entry was written to mongoDB!")
            pass