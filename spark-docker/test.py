
import pymongo
from datetime import date, datetime, timedelta
import pytz
import pandas as pd


now = datetime.now(tz = pytz.timezone("Europe/Berlin"))
two_hours_ago = now + timedelta(hours = -2)
request_timestamp = int(two_hours_ago.strftime("%y%m%d%H"))

test = int((now + timedelta(hours = 1)).strftime("%y%m%d%H"))



client = pymongo.MongoClient(host = localhost, port = 27000)
db = client.bahn


asd = list(db.timetable.aggregate([
    {"$match": {"timestamp": test}},
    {"$lookup":
        {
            "from": "changes",
            "localField": "@id",
            "foreignField": "@id",
            "as": "changes"
            }
        },
    {"$unwind": { # "flatten" list with only one element
        "path": "$changes",
        "preserveNullAndEmptyArrays": True
        }},
    # {"$unwind": { 
    #     "path": "$changes.dp.m",
    #     "preserveNullAndEmptyArrays": False
    #     }},
    {"$project": 
        {"_id": 0,
         "tl.@f": 1,
         "tl.@n": 1,
         "tl.@c": 1,
         "dp.@pt": 1, 
         "changes.dp.@ct": 1, 
         "changes.dp.m": 1
         }}
    ]))

pd.json_normalize(asd)

#! Spark Streaming
# Spark 3.1.2
# spark-sql-kafka-0-10_2.12-3.1.2
# Kafka 2.8.0

import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("myApp") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

initDf = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("startingOffsets", "earliest") \
  .option("subscribe", "changes") \
  .load()

result = initDf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

ds = result \
  .writeStream \
  .outputMode("update") \
  .format("console") \
  .start() \
  .awaitTermination()

  
query = (initDf
  .writeStream
  .outputMode("update")
  .format("memory")
  .queryName("some_name")
  .start())

spark.table("some_name").show()

initDf \
  .writeStream \
  .outputMode("update") \
  .format("console") \
  .start() \
  .awaitTermination()
  
  # .select(col("value").cast("string"))



# ! Spark

import pyspark
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession

print(pyspark.__version__)

conf = pyspark.SparkConf().set("spark.jars.packages",
                               "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
                            .setMaster("local") \
                            .setAppName("My App") \
                            .setAll([("spark.driver.memory", "5g"), ("spark.executor.memory", "5g")])
# setMaster: We run Spark on local machine
# setAll: Liste mit Parametern


sc = SparkContext(conf = conf)

sqlC = SQLContext(sc)

mongo_ip = "mongodb://localhost:27000/bahn."

spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", mongo_ip + "timetable") \
    .config("spark.mongodb.output.uri", mongo_ip + "timetable") \
    .getOrCreate()

bahn = sqlC.read.format("com.mongodb.spark.sql.DefaultSource").option("uri", mongo_ip + "timetable").load()

bahn.createOrReplaceTempView("bahn") # wenn wir das nicht machen, können wir den nächsten sql-query nicht ausführen

bahn = sqlC.sql("SELECT * FROM bahn")



pipeline = f"{{$match: {{timestamp: {test}}}}}"

pipeline2 = "{{$lookup: \
        {{ \
            from: changes, \
            localField: @id, \
            foreignField: @id, \
            as: changes \
            }} \
        }}"
    # {$unwind: { # flatten list with only one element
    #     path: $changes,
    #     preserveNullAndEmptyArrays: True
    #     }},
    # {$project: 
    #     {_id: 0,
    #      tl: 1, 
    #      dp.@pt: 1, 
    #      changes.dp.@ct: 1, 
    #      changes.dp.m: 1
    #      }}


df = spark.read.format("mongo").option("uri", mongo_ip + "timetable").option("pipeline", pipeline).load()

df = spark.read.format("mongo").option("uri", mongo_ip + "timetable").option("pipeline", pipeline).option("pipeline2", pipeline2).load()

df.take(10)

df.show()









#*SPARK

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("First App") \
    .getOrCreate()


rdd = spark.sparkContext.parallelize([1, 2, 3])

rdd.count()

rdd.map(lambda x: x**2).collect()

spark.table(rdd)


input_uri = "mongodb://127.0.0.1:27000/bahn.timetable"
output_uri = "mongodb://127.0.0.1:27000/bahn.timetable"

# connector: 2.11:2.4.2 / 2.12:3.0.1
# .config(spark.driver.extraClassPath, jars/*) \

spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", input_uri) \
    .config("spark.mongodb.output.uri", output_uri) \
    .getOrCreate()

    #.config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \

df = spark.read.format("mongo").load()







    
    
    
    
    
df = spark.read.format("mongo").option("uri", "mongodb://127.0.0.1/bahn.timetable").load()



#! Anleitung: https://www.sicara.ai/blog/2017-05-02-get-started-pyspark-jupyter-notebook-3-minutes

import findspark
findspark.init()

from pyspark.sql import SparkSession

input_uri = "mongodb://127.0.0.1/bahn.timetable"
output_uri = "mongodb://127.0.0.1/bahn.timetable"

# connector: 2.11:2.4.2 / 2.12:3.0.1
# .config(spark.driver.extraClassPath, jars/*) \

spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", input_uri) \
    .config("spark.mongodb.output.uri", output_uri) \
    .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1") \
    .getOrCreate()
    
spark.sparkContext.addPyFile("/home/daniel/Schreibtisch/Projekte/kafka-spark/spark-lokal/spark-3.0.2-bin-hadoop2.7/jars/bson-3.8.1.jar")

df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

#!sucht auf port 27017!!

print(df.printSchema())


./bin/pyspark --conf "spark.mongodb.input.uri=mongodb://127.0.0.1/bahn.timetable" --conf "spark.mongodb.output.uri=mongodb://127.0.0.1/bahn.timetable" --packages org.mongodb.spark:mongo-spark-connector_2.12:3.0.1





# * POSTGRES

import pandas as pd
from sqlalchemy import create_engine
import re


def connect_to_database():
    #! Noch ändern, wenn in Docker
    HOST = "localhost"
    PORT = 5555
    USER = "postgres"
    PASSWORD = "postgres"
    DB = "bahn"

    conn_string = f"postgres://{USER}:{PASSWORD}@{HOST}:{PORT}/{DB}"
    conn = create_engine(conn_string, echo = True).connect()
    
    return conn

conn = connect_to_database()
# query = """SELECT DISTINCT * FROM timetable LEFT JOIN (SELECT DISTINCT * FROM changes) as changes ON timetable.stop_id = changes.stop_id;"""

def fetch_from_postgres(connection, table_name):
  query = f"SELECT * FROM {table_name};"
  result = connection.execute(query)
  df = pd.DataFrame(result, columns = result.keys())
  return df

timetable_df = fetch_from_postgres(conn, "timetable")

changes_df = fetch_from_postgres(conn, "changes")
changes_df = changes_df.dropna(subset = ["ct"]).sort_values(by = "timestamp", ascending = False).groupby("stop_id").head(1)

#! Jede Stunde einen DAG machen, der mehr als 24 Stunden alte Einträge aus den Zwischendatenbanken entfernt

# * Left join

result = pd.merge(timetable_df.drop("timestamp", axis = 1), changes_df, on = "stop_id", how = "left")
# result = result.astype({"pt": float, "ct": float})

result["ct"].fillna(result["pt"], inplace = True) # Bei den, die bei ct NAN stehen haben, soll die entsprechende pt eingetragen werden

def convert_to_datetime(column):
    out = column.str.replace(r"(\w\w)(\w\w)(\w\w)(\w\w\w\w)", r"\1-\2-\3 \4").apply(pd.to_datetime, yearfirst = True)
    return out
    
result["pt"] = convert_to_datetime(result["pt"])
result["ct"] = convert_to_datetime(result["ct"])

result["delay"] = ((result["ct"] - result["pt"]).dt.total_seconds() / 60)

# Upsert in die finale Datenbank
def upsert_into_postgres(connection, df):
    create_table_query = """CREATE TABLE IF NOT EXISTS delays (
        stop_id TEXT PRIMARY KEY,
        c TEXT,
        f TEXT,
        n TEXT,
        pt TIMESTAMP WITHOUT TIME ZONE,
        ct TIMESTAMP WITHOUT TIME ZONE,
        code TEXT,
        timestamp TIMESTAMP WITHOUT TIME ZONE,
        delay DOUBLE PRECISION
        );"""
    connection.execute(create_table_query)

    df.to_sql(name = "temp_table", con = connection, 
              if_exists = "replace", index = False)

    colnames = list(df.columns)
    conflict_subquery = ",".join([f"{col} = EXCLUDED.{col}" for col in colnames])
    
    query = f"""INSERT INTO delays (SELECT * FROM temp_table) ON CONFLICT (stop_id) DO UPDATE SET {conflict_subquery};"""
    connection.execute(query)

upsert_into_postgres(conn, result)



#* Delete from database

import pymongo
from datetime import date, datetime, timedelta
import pytz
import pandas as pd

def delete_old_entries(connection, table_name):
    now = datetime.now(tz = pytz.timezone("Europe/Berlin"))
    one_day_ago = (now + timedelta(hours = -24)).astimezone(pytz.utc)
    
    query = f"DELETE FROM {table_name} WHERE timestamp < TIMESTAMP '{one_day_ago}'"
    
    connection.execute(query)