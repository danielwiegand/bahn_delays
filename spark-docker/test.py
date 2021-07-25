
import pymongo
from datetime import date, datetime, timedelta
import pytz
import pandas as pd


now = datetime.now(tz = pytz.timezone("Europe/Berlin"))
two_hours_ago = now + timedelta(hours = -2)
request_timestamp = int(two_hours_ago.strftime("%y%m%d%H"))

test = int((now + timedelta(hours = 1)).strftime("%y%m%d%H"))



client = pymongo.MongoClient(host = 'localhost', port = 27000)
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



pipeline = f"{{'$match': {{'timestamp': {test}}}}}"

pipeline2 = "{{'$lookup': \
        {{ \
            'from': 'changes', \
            'localField': '@id', \
            'foreignField': '@id', \
            'as': 'changes' \
            }} \
        }}"
    # {'$unwind': { # 'flatten' list with only one element
    #     'path': '$changes',
    #     'preserveNullAndEmptyArrays': True
    #     }},
    # {'$project': 
    #     {'_id': 0,
    #      'tl': 1, 
    #      'dp.@pt': 1, 
    #      'changes.dp.@ct': 1, 
    #      'changes.dp.m': 1
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
# .config('spark.driver.extraClassPath', 'jars/*') \

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
# .config('spark.driver.extraClassPath', 'jars/*') \

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