from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from functions import send_timetable_to_kafka, send_timetable_to_mongo


# * AIRFLOW ################################

# define default arguments
default_args = {
    'owner': 'daniel',
    'start_date': datetime(2021, 5, 9),
    # 'end_date':
    'email': ['an0nymus@posteo.de'],
    'email_on_failure': False,
    'email_on_retry': False,
    "retries": 3,
    "retry_delay": timedelta(minutes = 1)
}

# instantiate a DAG
dag = DAG('timetable', description = '', catchup = False,
          schedule_interval = timedelta(minutes = 5), default_args = default_args)

# define task
t1 = PythonOperator(task_id = 'send_timetable_to_kafka', 
                    python_callable = send_timetable_to_kafka,
                    dag = dag)
t2 = PythonOperator(task_id = 'send_timetable_to_mongo', 
                    python_callable = send_timetable_to_mongo,
                    dag = dag)


# setup dependencies
t1 >> t2 
