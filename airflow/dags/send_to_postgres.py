from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
import pyspark.sql.functions as F
from pyspark.sql.types import StructField, StructType, StringType

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("myApp") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")


# * TIMETABLE

timetable_stream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka-server:9092") \
  .option("startingOffsets", "latest") \
  .option("subscribe", "timetable,changes") \
  .load()
  
schema_timetable = StructType(
        [
                StructField("@id", StringType()),
                StructField("tl", StructType([
                    StructField("@f", StringType()),
                    StructField("@c", StringType()),
                    StructField("@n", StringType())
                    ])),
                StructField("dp", StructType([
                    StructField("@pt", StringType())
                    ]))
                ]
        )

timetable_result = timetable_stream.selectExpr("CAST(value AS STRING)") \
    .withColumn("value", from_json("value", schema_timetable)) \
    .select(col("value.@id").alias("stop_id"), col("value.tl.@c").alias("c"), col("value.tl.@f").alias("f"), col("value.tl.@n").alias("n"), col("value.dp.@pt").alias("pt")) \
    .distinct()
    

# * CHANGES

changes_stream = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "kafka-server:9092") \
  .option("startingOffsets", "latest") \
  .option("subscribe", "changes") \
  .load()
      
    
schema_changes = StructType(
        [
                StructField("@id", StringType()),
                StructField("dp", StructType([
                    StructField("@ct", StringType()),
                    StructField("m", StructType([
                        StructField("@c", StringType())
                    ]))
                ]))
        ]
)

changes_result = changes_stream.selectExpr("CAST(value AS STRING)") \
    .withColumn("value", from_json("value", schema_changes)) \
    .select(col("value.@id").alias("stop_id"), col("value.dp.@ct").alias("ct"), col("value.dp.m.@c").alias("code")) \
    .distinct()
    

# ds = timetable_result \
#   .writeStream \
#   .outputMode("append") \
#   .format("console") \
#   .start()
  

# * WRITE TO POSTGRES

DB = "bahn"
USER = "postgres"
PASSWORD = "postgres"
HOST = "postgres_streams"
PORT = 5432
    
CONN_STRING = f"jdbc:postgresql://{HOST}:{PORT}/{DB}"

PROPERTIES = {
  "driver": "org.postgresql.Driver",
  "user": USER,
  "password": PASSWORD
}

def postgres_sink(data_frame, batch_id, table_name):

  data_frame.write.jdbc(url = CONN_STRING, 
                        table = table_name, 
                        mode = "append", 
                        properties = PROPERTIES)


write_changes = changes_result \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(lambda df, epochId: postgres_sink(df, epochId, "changes")) \
    .start()

write_timetable = timetable_result \
    .writeStream \
    .outputMode("append") \
    .foreachBatch(lambda df, epochId: postgres_sink(df, epochId, "timetable")) \
    .start()
    
spark.streams.awaitAnyTermination()
