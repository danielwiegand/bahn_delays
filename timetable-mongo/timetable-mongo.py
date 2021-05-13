import pymongo
import json
from kafka import KafkaConsumer

# * MONGODB ##############################

# create a connection
client = pymongo.MongoClient(host = 'mongo', port = 27017)

# create/or connect to a database
db = client.bahn


# * KAFKA #################################

consumer = KafkaConsumer("timetable",
                         bootstrap_servers = "kafka-server:9092",
                         auto_offset_reset = 'earliest',
                         enable_auto_commit = True, # If True , the consumer’s offset will be periodically committed in the background.
                         value_deserializer = lambda m: json.loads(m.decode('utf-8')),
                         group_id = 'my-group-1')


# * WRITE TO MONGODB ######################

for timetable in consumer:

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