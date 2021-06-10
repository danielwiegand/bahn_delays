#from pyspark import SparkContext
#sc = SparkContext("local", "First App")

# SIEHE https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession \
    .builder \
    .appName("First App") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN") # Hide the INFO messages

# READ

# df = spark \
#   .read \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "kafka-server:9092") \
#   .option("subscribe", "topic1") \
#   .load()

# df.selectExpr("CAST(value AS STRING)").show()


# READSTREAM

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka-server:9092") \
  .option("subscribe", "topic1") \
  .load()

msg = df.selectExpr("CAST(value AS STRING)")

disp = msg.writeStream.outputMode("append").format("console").start().awaitTermination()



# Split the lines into words
# words = lines.select(
#    explode(
#        split(lines.value, " ")
#    ).alias("word")
# )

# Generate running word count
# wordCounts = words.groupBy("word").count()


 # Start running the query that prints the running counts to the console 
# query = wordCounts \
#     .writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .start()

# query.awaitTermination() # it just waits for the termination signal from user. When it receives signal from user (i.e CTRL+C or SIGTERM) then it streaming context will be stoppe