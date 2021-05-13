import requests
import json, xmltodict
from kafka import KafkaProducer
import time


# XML TO JSON
def convert_to_json(txt):
  return xmltodict.parse(txt)


# * KAFKA #################################

# KAFKA PRODUCER

producer = KafkaProducer(bootstrap_servers = "kafka-server:9092",
                         value_serializer = lambda m: json.dumps(m).encode('utf-8')
                         )


# * TIMETABLE #############################

headers = {"Authorization": "Bearer c2717c0f768243e30011b8b3104f6d3d",
           "Accept": "applica tion/xml"}

eva = 8004158 # 8000261: München, 8003680: Limburg Süd, 8004158: Pasing

url = f"https://api.deutschebahn.com/timetables/v1/rchg/{eva}"

r = requests.get(url, headers = headers)

if r.status_code == 200:

  recent_changes = convert_to_json(r.text)
  
  for change in recent_changes["timetable"]["s"]:
    producer.send(
      topic = "changes", 
      value = change
      )

  time.sleep(1)
