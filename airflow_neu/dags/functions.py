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
now = datetime.now()

eva = 8004158
today_date = int(today.strftime("%y%m%d"))
hour = int(now.strftime("%H")) + 1


# XML TO JSON
def convert_to_json(txt):
  return xmltodict.parse(txt)


# * TIMETABLE TO KAFKA #############################

def prepare_url(eva, today_date, hour):
    return f"https://api.deutschebahn.com/timetables/v1/plan/{eva}/{today_date}/{hour}"

def make_request(url):
    
    producer = KafkaProducer(bootstrap_servers = "kafka-server:9092",
                         value_serializer = lambda m: json.dumps(m).encode('utf-8')
                         )

    r = requests.get(url, headers = HEADERS)
    if r.status_code == 200:
        tt = convert_to_json(r.text)
        for train_stop in tt["timetable"]["s"]:
            print(train_stop)
            producer.send(topic = "timetable",
                          value = train_stop)
            time.sleep(1)
    else:
        print(f"ERROR: Could not fetch data from Deutsche Bahn: {r.content}")

def send_timetable_to_kafka():
    url = prepare_url(eva, date, hour)
    make_request(url)
    
    
# * TIMETABLE TO MONGODB #############################

def connect_to_mongo():
    # create a connection
    client = pymongo.MongoClient(host = 'mongo', port = 27017)

    # create/or connect to a database
    db = client.bahn
    
    return db

def send_timetable_to_mongo():

    consumer = KafkaConsumer("timetable",
                            bootstrap_servers = "kafka-server:9092",
                            auto_offset_reset = 'earliest',
                            enable_auto_commit = True, # If True , the consumer’s offset will be periodically committed in the background.
                            value_deserializer = lambda m: json.loads(m.decode('utf-8')),
                            group_id = 'my-group-1')
    
    print(consumer)
    
    for text in consumer:
        logging.critical(text)
    
    db = connect_to_mongo()
    
    print(db)

    for timetable in consumer:
        
        logging.critical(timetable)

        try:
            db.timetable.update(
                {"@id": timetable.value["@id"]},
                timetable.value,
                upsert = True
                )
        except TypeError:
            print("Type error occured while timetable was written to mongoDB!")
            pass


# TODO: Streaming oder batch? Besser: streaming!
# TODO: Offsets?
