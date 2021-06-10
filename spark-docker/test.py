#*Mongodb

import pymongo

# create a connection
client = pymongo.MongoClient(host = 'localhost', port = 27000)

db = client.bahn

list(db.timetable.find())


#*SPARK

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("First App") \
    .getOrCreate()
    
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