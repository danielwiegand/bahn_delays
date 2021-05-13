from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from functions import send_to_kafka, send_to_mongo


# * AIRFLOW ################################

# default arguments
default_args = {
    'owner': 'daniel',
    'start_date': datetime(2021, 5, 9),
    'email': ['an0nymus@posteo.de'],
    'email_on_failure': False,
    'email_on_retry': False,
    "retries": 3,
    "retry_delay": timedelta(minutes = 1)
}

# timetable DAG
dag_timetable = DAG('timetable', 
                    description = 'Fetch timetable data hourly', catchup = False, schedule_interval = "@hourly", default_args = default_args)

t1 = PythonOperator(task_id = "send_timetable_to_kafka", 
                    python_callable = send_to_kafka,
                    op_kwargs = {"topic": "timetable"},
                    dag = dag_timetable)
t2 = PythonOperator(task_id = 'send_timetable_to_mongo', 
                    python_callable = send_to_mongo,
                    op_kwargs = {"topic": "timetable"},
                    dag = dag_timetable)

t1 >> t2 # dependencies


# changes DAG
dag_changes = DAG('changes', 
                  description = 'Fetch the recent timetable changes every 2 minutes', catchup = False, schedule_interval = timedelta(seconds = 119), default_args = default_args)

c1 = PythonOperator(task_id = "send_changes_to_kafka", 
                    python_callable = send_to_kafka,
                    op_kwargs = {"topic": "changes"},
                    dag = dag_changes)
c2 = PythonOperator(task_id = 'send_changes_to_mongo', 
                    python_callable = send_to_mongo,
                    op_kwargs = {"topic": "changes"},
                    dag = dag_changes)

c1 >> c2 # dependencies
