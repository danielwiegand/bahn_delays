import requests
import json, xmltodict
from kafka import KafkaProducer
import time
from datetime import date, datetime


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
           "Accept": "application/xml"}

today = date.today()
now = datetime.now()

eva = 8004158
today_date = int(today.strftime("%y%m%d"))
hour = int(now.strftime("%H")) + 1

url = f"https://api.deutschebahn.com/timetables/v1/plan/{eva}/{today_date}/{hour}"

r = requests.get(url, headers = headers)

if r.status_code == 200:

  tt = convert_to_json(r.text)
  
  for train_stop in tt["timetable"]["s"]:
      
      producer.send(topic = "timetable",
                    value = train_stop)
      
      time.sleep(1)
      
else:
  
  print(f"ERROR: Could not fetch data from Deutsche Bahn: {r.content}")
      