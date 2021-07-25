from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

spark = SparkSession \
    .builder \
    .appName("test") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# * CHANGES

left_stream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka-server:9092") \
  .option("startingOffsets", "latest") \
  .option("subscribe", "left") \
  .load()

right_stream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka-server:9092") \
  .option("startingOffsets", "latest") \
  .option("subscribe", "right") \
  .load()
  
schema_left = StructType(
    [
            StructField("id_left", IntegerType()),
            StructField("pt", StringType())
            ]
    )

schema_right = StructType(
    [
            StructField("id_right", IntegerType()),
            StructField("ct", StringType())
            ]
    )

# {"id_left": 6, "pt": "10"}
# {"id_right": 6, "ct": "20"}
# {"id_right": 5, "ct": "30"}


left_result = left_stream.selectExpr("CAST(value AS STRING)") \
    .withColumn("value", from_json("value", schema_left)) \
    .select("value.*") \
    .withColumn("ts_left", F.current_timestamp())
    
right_result = right_stream.selectExpr("CAST(value AS STRING)") \
    .withColumn("value", from_json("value", schema_right)) \
    .select("value.*") \
    .withColumn("ts_right", F.current_timestamp())
    
    
from pyspark.sql.functions import expr

# Apply watermarks on event-time columns
leftWithWatermark = left_result.withWatermark("ts_left", "0 seconds")
rightWithWatermark = right_result.withWatermark("ts_right", "0 seconds")

join = leftWithWatermark.join(
  rightWithWatermark,
  expr(""" 
       id_left = id_right AND 
       ts_left <= ts_right AND
       ts_left >= ts_right - interval 15 seconds
    """
  ),
  "left"
)
    
ds = join \
  .writeStream \
  .outputMode("append") \
  .format("console") \
  .start() \
  .awaitTermination()