from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType, ArrayType

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("myApp") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# * CHANGES

changes_stream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka-server:9092") \
  .option("startingOffsets", "latest") \
  .option("subscribe", "changes") \
  .load()
      
      
# Schema für Code als Einzelwert (ohne Array)
# schema_changes = StructType(
#         [
#                 StructField("@id", StringType()),
#                 StructField("dp", StructType([
#                     StructField("@ct", StringType()),
#                     StructField("m", StructType([
#                         StructField("@c", StringType())
#                     ]))
#                 ]))
#         ]
# )
   
# # Schema für Code innerhalb eines Arrays 
# schema_changes = StructType(
#         [
#                 StructField("@id", StringType()),
#                 StructField("dp", StructType([
#                     StructField("@ct", StringType()),
#                     StructField("m", ArrayType(
#                         StructType([
#                             StructField("@c", StringType())
#                         ])
#                     ))
#                 ]))
#         ]
# )

# Schema für "m" als reiner String
schema_changes = StructType(
        [
                StructField("@id", StringType()),
                StructField("dp", StructType([
                    StructField("@ct", StringType()),
                    StructField("m", StringType())
                ]))
        ]
)


#! Vllt versuchen: Erst als String speichern (Hauptsache raus aus Spark) und das dann später handeln? Beim Join? Oder mit regex "brute force" rausholen?

# changes_result = changes_stream.selectExpr("CAST(value AS STRING)") \
#     .withColumn("value", from_json("value", schema_changes)) \
#     .select(col("value.@id").alias("stop_id"), col("value.dp.@ct").alias("ct"), col("value.dp.m.@c").alias("code")) \
#     .withColumn("timestamp", F.current_timestamp()) \
#     .distinct()

changes_result = changes_stream.selectExpr("CAST(value AS STRING)") \
    .withColumn("value", from_json("value", schema_changes)) \
    .select(col("value.@id").alias("stop_id"), col("value.dp.@ct").alias("ct"), col("value.dp.m").alias("code")) \
    .withColumn("timestamp", F.current_timestamp()) \
    .distinct()


ds = changes_result \
  .writeStream \
  .outputMode("append") \
  .format("console") \
  .start() \
  .awaitTermination()
  
# Beispiel
# {"@id": "TEST", "@eva": "8004158", "dp": {"@ct": "2107291837", "@l": "3", "m": [{"@id": "r94912549", "@t": "d", "@c": "43", "@ts": "2107291741", "@ts-tts": "21-07-29 17:41:35.860"}, {"@id": "r94913602", "@t": "d", "@c": "41", "@ts": "2107291755", "@ts-tts": "21-07-29 17:55:25.345"}, {"@id": "r94913909", "@t": "d", "@c": "43", "@ts": "2107291800", "@ts-tts": "21-07-29 18:00:20.731"}, {"@id": "r94913930", "@t": "d", "@c": "41", "@ts": "2107291800", "@ts-tts": "21-07-29 18:00:20.731"}, {"@id": "r94914076", "@t": "d", "@c": "41", "@ts": "2107291802", "@ts-tts": "21-07-29 18:02:11.079"}, {"@id": "r94914819", "@t": "d", "@c": "41", "@ts": "2107291811", "@ts-tts": "21-07-29 18:11:07.751"}]}}


# {"@id": "TEST", "@eva": "8004158", "dp": {"@ct": "2107291837", "@l": "3", "m": {"@id": "r94912549", "@t": "d", "@c": "43", "@ts": "2107291741", "@ts-tts": "21-07-29 17:41:35.860"}}}


