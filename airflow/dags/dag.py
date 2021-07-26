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

# * timetable DAG
dag_timetable = DAG('timetable', 
                    description = 'Fetch timetable data hourly', catchup = False, schedule_interval = "@hourly", default_args = default_args)

t1 = PythonOperator(task_id = "send_timetable_to_kafka", 
                    python_callable = send_to_kafka,
                    op_kwargs = {"topic": "timetable"},
                    dag = dag_timetable)


# * changes DAG
dag_changes = DAG('changes', 
                  description = 'Fetch the recent timetable changes every 2 minutes', catchup = False, schedule_interval = timedelta(seconds = 119), default_args = default_args)

c1 = PythonOperator(task_id = "send_changes_to_kafka", 
                    python_callable = send_to_kafka,
                    op_kwargs = {"topic": "changes"},
                    dag = dag_changes)



# * spark DAG

from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

dag_spark = DAG('spark', 
                description = '', catchup = False, schedule_interval = "@once", default_args = default_args)

s1 = SparkSubmitOperator(
    task_id = "spark-job",
    application = "/opt/airflow/dags/send_to_postgres.py",
    conn_id = "spark_default", # defined under Admin/Connections in Airflow webserver
    packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2,postgresql:postgresql:9.1-901-1.jdbc4",
    dag = dag_spark
)


# * join DAG
# dag_join = DAG('join_timetable_changes', 
#                     description = 'Join timetable and changes and load into new database', catchup = False, schedule_interval = "@hourly", default_args = default_args)

# j1 = PythonOperator(task_id = "join_timetable_changes", 
#                     python_callable = join_timetable_changes,
#                     # op_kwargs = {"topic": "timetable"},
#                     dag = dag_join)