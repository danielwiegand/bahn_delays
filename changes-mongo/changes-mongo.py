import pymongo
import json
from kafka import KafkaConsumer

# * MONGODB ##############################

# create a connection
client = pymongo.MongoClient(host = 'mongo', port = 27017)

# create/or connect to a database
db = client.bahn


# * KAFKA #################################

consumer = KafkaConsumer("changes",
                         bootstrap_servers = "kafka-server:9092",
                         auto_offset_reset = 'earliest',
                         enable_auto_commit = True, # If True , the consumer’s offset will be periodically committed in the background.
                         value_deserializer = lambda m: json.loads(m.decode('utf-8')),
                         group_id = 'my-group-1')


# * WRITE TO MONGODB ######################

for train_stop in consumer:

    try:
      db.changes.update(
        {"@id": train_stop.value["@id"]},
        train_stop.value,
        upsert = True
        )
    except TypeError:
      print("Type error occured while train_stop was written to mongoDB!")
      pass


# TODO: Streaming oder batch? Besser: streaming!
# TODO: Offsets?