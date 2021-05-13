import json
import time
from datetime import date, datetime, timedelta

import requests
import xmltodict
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from kafka import KafkaProducer
import logging


# XML TO JSON
def convert_to_json(txt):
  return xmltodict.parse(txt)


# # * KAFKA #################################

# # KAFKA PRODUCER

# producer = KafkaProducer(bootstrap_servers = "kafka-server:9092",
#                          value_serializer = lambda m: json.dumps(m).encode('utf-8')
#                          )


# # * TIMETABLE #############################

# HEADERS = {"Authorization": "Bearer c2717c0f768243e30011b8b3104f6d3d",
#            "Accept": "application/xml"}

# today = date.today()
# now = datetime.now()

# eva = 8004158
# today_date = int(today.strftime("%y%m%d"))
# hour = int(now.strftime("%H")) + 1

# def prepare_url(eva, date, hour):
#     return f"https://api.deutschebahn.com/timetables/v1/plan/{eva}/{today_date}/{hour}"

# def make_request(url):  
#     r = requests.get(url, headers = HEADERS)
#     if r.status_code == 200:
#         tt = convert_to_json(r.text)
#         for train_stop in tt["timetable"]["s"]:
#             producer.send(topic = "timetable",
#                           value = train_stop)
#             time.sleep(1)
#     else:
#         print(f"ERROR: Could not fetch data from Deutsche Bahn: {r.content}")

# def send_timetable_to_kafka():
#     url = prepare_url(eva, date, hour)
#     make_request(url)

def test_function():
    logging.critical("DAS IST EIN TEST!")

logging.critical("ICH BIN HIER!")


# * AIRFLOW ################################

# define default arguments
default_args = {
    'owner': 'daniel',
    'start_date': datetime(2021, 5, 9),
    # 'end_date':
    'email': ['an0nymus@posteo.de'],
    'email_on_failure': True,
    'email_on_retry': True,
    "retries": 3,
    "retry_delay": timedelta(minutes = 1)
}

# instantiate a DAG
dag = DAG('timetable', description = '', catchup = False,
          schedule_interval = timedelta(minutes = 1), default_args = default_args)

# define task
t1 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag = dag
)
t2 = PythonOperator(task_id = 'send_kafka', 
                    python_callable = test_function,
                    dag = dag)


# setup dependencies
t1 >> t2 
# t2 >> t4 >> t5
