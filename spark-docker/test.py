
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

#! Upsert mit foreach: https://docs.databricks.com/_static/notebooks/merge-in-streaming.html
#! Timestamp einbauen bei timetable + changes

import pandas as pd
from sqlalchemy import create_engine

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
query = """SELECT DISTINCT * FROM timetable LEFT JOIN (SELECT DISTINCT * FROM changes) as changes ON timetable.stop_id = changes.stop_id;"""

query = """SELECT * FROM changes;"""

joined_data = conn.execute(query)
joined_data_df = pd.DataFrame(joined_data)

joined_data_df.iloc[:,0].value_counts()

joined_data_df.loc[joined_data_df.iloc[:,0] == "5111774224362867641-2107261718-4"]






import re
from collections import Counter
ids = re.findall("s id=\"(.+)\"", text)
Counter(ids)

text = '''<timetable station=M&#252;nchen-Pasing>
  <s id="7537395545451740415-2107261728-2">
    <tl f="F" t="p" o="80" c="ICE" n="512"/>
    <ar pt="2107261735" pp="10" hi="1" ppth="M&#252;nchen Hbf"/>
    <dp pt="2107261736" pp="10" ppth="Augsburg Hbf|Ulm Hbf|Stuttgart Hbf|Mannheim Hbf|Frankfurt(M) Flughafen Fernbf|Siegburg/Bonn|K&#246;ln Hbf"/>
  </s>
  <s id="4976704969139597363-2107261656-2">
    <tl f="N" t="p" o="800790" c="RB" n="59459"/>
    <ar pt="2107261703" pp="4" l="6" wings="2646931618398905440-2107261656" ppth="M&#252;nchen Hbf Gl.27-36"/>
    <dp pt="2107261704" pp="4" l="6" wings="2646931618398905440-2107261656" ppth="Starnberg|Tutzing|Weilheim(Oberbay)|Huglfing|Uffing a Staffelsee|Murnau|Ohlstadt|Eschenlohe|Oberau|Farchant|Garmisch-Partenkirchen"/>
  </s>
  <s id="-8689284970282716918-2107261741-6">
    <tl f="S" t="p" o="800725" c="S" n="8879"/>
    <ar pt="2107261752" pp="5" l="8" ppth="Germering-Unterpfaffenhofen|Harthaus|M&#252;nchen-Freiham|M&#252;nchen-Neuaubing|M&#252;nchen-Westkreuz"/>
    <dp pt="2107261755" pp="5" l="8" ppth="M&#252;nchen-Laim|M&#252;nchen Hirschgarten|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Hbf (tief)|M&#252;nchen Karlsplatz|M&#252;nchen Marienplatz|M&#252;nchen Isartor|M&#252;nchen Rosenheimer Platz|M&#252;nchen Ost"/>
  </s>
  <s id="-8771616788815580336-2107261709-8">
    <tl f="S" t="p" o="800725" c="S" n="8166"/>
    <ar pt="2107261730" pp="2" l="20" ppth="H&#246;llriegelskreuth|Pullach|Gro&#223;hesselohe Isartalbf|M&#252;nchen-Solln|M&#252;nchen Siemenswerke|M&#252;nchen-Mittersendling|M&#252;nchen Heimeranplatz"/>
  </s>
  <s id="2005535188249917016-2107261732-2">
    <tl f="N" t="p" o="800790" c="RB" n="5532"/>
    <ar pt="2107261738" pp="4" l="60" ppth="M&#252;nchen Hbf Gl.27-36"/>
    <dp pt="2107261738" pp="4" l="60" ppth="Tutzing|Weilheim(Oberbay)|Huglfing|Uffing a Staffelsee|Murnau|Ohlstadt|Eschenlohe|Oberau|Farchant|Garmisch-Partenkirchen|Garmisch-Partenkirchen Hausberg|Untergrainau|Ehrwald Zugspitzbahn|Lermoos|L&#228;hn|Bichlbach-Berwang|Heiterwang-Plansee|Reutte in Tirol Schulzentrum|Reutte in Tirol|Pflach|Musau|Ulrichsbr&#252;cke-F&#252;ssen|Vils Stadt"/>
  </s>
  <s id="5555506748177717585-2107261647-11">
    <tl f="S" t="p" o="800725" c="S" n="8872"/>
    <ar pt="2107261705" pp="7" l="8" ppth="M&#252;nchen Ost|M&#252;nchen Rosenheimer Platz|M&#252;nchen Isartor|M&#252;nchen Marienplatz|M&#252;nchen Karlsplatz|M&#252;nchen Hbf (tief)|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hirschgarten|M&#252;nchen-Laim"/>
    <dp pt="2107261706" pp="7" l="8" ppth="M&#252;nchen-Westkreuz|M&#252;nchen-Neuaubing|M&#252;nchen-Freiham|Harthaus|Germering-Unterpfaffenhofen"/>
  </s>
  <s id="5254114651454294172-2107260919-14">
    <tl f="F" t="p" o="80" c="ICE" n="597"/>
    <ar pt="2107261718" pp="9" ppth="Berlin Gesundbrunnen|Berlin Hbf (tief)|Berlin S&#252;dkreuz|Lutherstadt Wittenberg Hbf|Leipzig Hbf|Erfurt Hbf|Eisenach|Fulda|Frankfurt(Main)Hbf|Mannheim Hbf|Stuttgart Hbf|Ulm Hbf|Augsburg Hbf"/>
    <dp pt="2107261720" pp="9" hi="1" ppth="M&#252;nchen Hbf"/>
  </s>
  <s id="1886987700573918331-2107261739-2">
    <tl f="N" t="p" o="8007D5" c="RE" n="57416"/>
    <ar pt="2107261746" pp="3" l="72" wings="-610045601169203856-2107261739" ppth="M&#252;nchen Hbf Gl.27-36"/>
    <dp pt="2107261747" pp="3" l="72" wings="-610045601169203856-2107261739" ppth="Geltendorf|Kaufering|Buchloe|T&#252;rkheim(Bay)Bf|Mindelheim|Sontheim(Schwab)|Memmingen"/>
  </s>
  <s id="6624707364840639029-2107261702-19">
    <tl f="S" t="p" o="800725" c="S" n="6686"/>
    <ar pt="2107261741" pp="7" l="6" ppth="Zorneding|Baldham|Vaterstetten|Haar|Gronsdorf|M&#252;nchen-Trudering|M&#252;nchen-Berg am Laim|M&#252;nchen Leuchtenbergring|M&#252;nchen Ost|M&#252;nchen Rosenheimer Platz|M&#252;nchen Isartor|M&#252;nchen Marienplatz|M&#252;nchen Karlsplatz|M&#252;nchen Hbf (tief)|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hirschgarten|M&#252;nchen-Laim"/>
    <dp pt="2107261743" pp="7" l="6" ppth="M&#252;nchen-Westkreuz|Lochham|Gr&#228;felfing|Planegg|Stockdorf|Gauting|Starnberg Nord|Starnberg|Possenhofen|Feldafing|Tutzing"/>
  </s>
  <s id="-8122987496998094286-2107261640-19">
    <tl f="S" t="p" o="800725" c="S" n="8378"/>
    <ar pt="2107261717" pp="8" l="3" ppth="Deisenhofen|Furth(b Deisenhofen)|Taufkirchen|Unterhaching|Fasanenpark|M&#252;nchen-Fasangarten|M&#252;nchen-Giesing|M&#252;nchen St.Martin-Str.|M&#252;nchen Ost|M&#252;nchen Rosenheimer Platz|M&#252;nchen Isartor|M&#252;nchen Marienplatz|M&#252;nchen Karlsplatz|M&#252;nchen Hbf (tief)|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hirschgarten|M&#252;nchen-Laim"/>
    <dp pt="2107261719" pp="8" l="3" ppth="M&#252;nchen-Langwied|M&#252;nchen-Lochhausen|Gr&#246;benzell|Olching|Esting|Gernlinden|Maisach"/>
  </s>
  <s id="8046937508250611568-2107261724-10">
    <tl f="S" t="p" o="800725" c="S" n="6391"/>
    <ar pt="2107261750" pp="6" l="3" ppth="Mammendorf|Malching(Oberbay)|Maisach|Gernlinden|Esting|Olching|Gr&#246;benzell|M&#252;nchen-Lochhausen|M&#252;nchen-Langwied"/>
    <dp pt="2107261753" pp="6" l="3" ppth="M&#252;nchen-Laim|M&#252;nchen Hirschgarten|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Hbf (tief)|M&#252;nchen Karlsplatz|M&#252;nchen Marienplatz|M&#252;nchen Isartor|M&#252;nchen Rosenheimer Platz|M&#252;nchen Ost|M&#252;nchen St.Martin-Str.|M&#252;nchen-Giesing|M&#252;nchen-Fasangarten|Fasanenpark|Unterhaching|Taufkirchen|Furth(b Deisenhofen)|Deisenhofen|Sauerlach|Otterfing|Holzkirchen"/>
  </s>
  <s id="5818557538583816226-2107261724-12">
    <tl f="S" t="p" o="800725" c="S" n="6691"/>
    <ar pt="2107261757" pp="6" l="6" ppth="Tutzing|Feldafing|Possenhofen|Starnberg|Starnberg Nord|Gauting|Stockdorf|Planegg|Gr&#228;felfing|Lochham|M&#252;nchen-Westkreuz"/>
    <dp pt="2107261758" pp="6" l="6" ppth="M&#252;nchen-Laim|M&#252;nchen Hirschgarten|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Hbf (tief)|M&#252;nchen Karlsplatz|M&#252;nchen Marienplatz|M&#252;nchen Isartor|M&#252;nchen Rosenheimer Platz|M&#252;nchen Ost|M&#252;nchen Leuchtenbergring|M&#252;nchen-Berg am Laim|M&#252;nchen-Trudering|Gronsdorf|Haar|Vaterstetten|Baldham|Zorneding|Eglharting|Kirchseeon|Grafing Bahnhof|Grafing Stadt|Ebersberg(Oberbay)"/>
  </s>
  <s id="-610045601169203856-2107261739-2">
    <tl f="N" t="p" o="8007D5" c="RE" n="57444"/>
    <ar pt="2107261746" pp="3" l="70" ppth="M&#252;nchen Hbf Gl.27-36"/>
    <dp pt="2107261747" pp="3" l="70" ppth="Geltendorf|Kaufering|Buchloe|Kaufbeuren|Biessenhofen|G&#252;nzach|Kempten(Allg&#228;u)Hbf"/>
  </s>
  <s id="-1628791440308860899-2107261752-2">
    <tl f="N" t="p" o="800734" c="RB" n="57096"/>
    <ar pt="2107261759" pp="10" l="87" ppth="M&#252;nchen Hbf"/>
    <dp pt="2107261800" pp="10" l="87" ppth="Mering|Augsburg-Hochzoll|Augsburg Haunstetterstra&#223;e|Augsburg Hbf|Augsburg-Oberhausen|Gersthofen|Gablingen|Langweid(Lech)|Herbertshofen|Meitingen|Westendorf|Nordendorf|Mertingen Bahnhof|B&#228;umenheim|Donauw&#246;rth"/>
  </s>
  <s id="-2518418949276411018-2107261644-12">
    <tl f="S" t="p" o="800725" c="S" n="6687"/>
    <ar pt="2107261717" pp="6" l="6" ppth="Tutzing|Feldafing|Possenhofen|Starnberg|Starnberg Nord|Gauting|Stockdorf|Planegg|Gr&#228;felfing|Lochham|M&#252;nchen-Westkreuz"/>
    <dp pt="2107261718" pp="6" l="6" ppth="M&#252;nchen-Laim|M&#252;nchen Hirschgarten|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Hbf (tief)|M&#252;nchen Karlsplatz|M&#252;nchen Marienplatz|M&#252;nchen Isartor|M&#252;nchen Rosenheimer Platz|M&#252;nchen Ost|M&#252;nchen Leuchtenbergring|M&#252;nchen-Berg am Laim|M&#252;nchen-Trudering|Gronsdorf|Haar|Vaterstetten|Baldham|Zorneding"/>
  </s>
  <s id="1609101522541513258-2107261616-22">
    <tl f="S" t="p" o="800725" c="S" n="6382"/>
    <ar pt="2107261707" pp="8" l="3" ppth="Holzkirchen|Otterfing|Sauerlach|Deisenhofen|Furth(b Deisenhofen)|Taufkirchen|Unterhaching|Fasanenpark|M&#252;nchen-Fasangarten|M&#252;nchen-Giesing|M&#252;nchen St.Martin-Str.|M&#252;nchen Ost|M&#252;nchen Rosenheimer Platz|M&#252;nchen Isartor|M&#252;nchen Marienplatz|M&#252;nchen Karlsplatz|M&#252;nchen Hbf (tief)|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hirschgarten|M&#252;nchen-Laim"/>
    <dp pt="2107261709" pp="8" l="3" ppth="M&#252;nchen-Langwied|M&#252;nchen-Lochhausen|Gr&#246;benzell|Olching|Esting|Gernlinden|Maisach|Malching(Oberbay)|Mammendorf"/>
  </s>
  <s id="836587502791763407-2107261648-7">
    <tl f="S" t="p" o="800725" c="S" n="6487"/>
    <ar pt="2107261706" pp="5" l="4" ppth="Buchenau(Oberbay)|F&#252;rstenfeldbruck|Eichenau(Oberbay)|Puchheim|M&#252;nchen-Aubing|M&#252;nchen Leienfelsstr."/>
    <dp pt="2107261709" pp="5" l="4" ppth="M&#252;nchen-Laim|M&#252;nchen Hirschgarten|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Hbf (tief)|M&#252;nchen Karlsplatz|M&#252;nchen Marienplatz|M&#252;nchen Isartor|M&#252;nchen Rosenheimer Platz|M&#252;nchen Ost|M&#252;nchen Leuchtenbergring|M&#252;nchen-Berg am Laim|M&#252;nchen-Trudering|Gronsdorf|Haar|Vaterstetten|Baldham|Zorneding|Eglharting|Kirchseeon|Grafing Bahnhof"/>
  </s>
  <s id="2646931618398905440-2107261656-2">
    <tl f="N" t="p" o="800790" c="RB" n="59645"/>
    <ar pt="2107261703" pp="4" l="66" ppth="M&#252;nchen Hbf Gl.27-36"/>
    <dp pt="2107261704" pp="4" l="66" ppth="Starnberg|Tutzing|Bernried|Seeshaupt|Iffeldorf|Penzberg|Bichl|Benediktbeuern|Kochel"/>
  </s>
  <s id="6523266399399988461-2107261712-2">
    <tl f="N" t="p" o="8007D5" c="RE" n="3892"/>
    <ar pt="2107261719" pp="4" l="76" ppth="M&#252;nchen Hbf Gl.27-36"/>
    <dp pt="2107261720" pp="4" l="76" ppth="Kaufering|Buchloe|Kaufbeuren|Kempten(Allg&#228;u)Hbf|Immenstadt|Sonthofen|Fischen|Oberstdorf"/>
  </s>
  <s id="350903202654360817-2107261707-11">
    <tl f="S" t="p" o="800725" c="S" n="8874"/>
    <ar pt="2107261725" pp="7" l="8" ppth="M&#252;nchen Ost|M&#252;nchen Rosenheimer Platz|M&#252;nchen Isartor|M&#252;nchen Marienplatz|M&#252;nchen Karlsplatz|M&#252;nchen Hbf (tief)|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hirschgarten|M&#252;nchen-Laim"/>
    <dp pt="2107261726" pp="7" l="8" ppth="M&#252;nchen-Westkreuz|M&#252;nchen-Neuaubing|M&#252;nchen-Freiham|Harthaus|Germering-Unterpfaffenhofen"/>
  </s>
  <s id="2841843197154732516-2107261625-13">
    <tl f="S" t="p" o="800725" c="S" n="6887"/>
    <ar pt="2107261702" pp="6" l="8" ppth="Herrsching|Seefeld-Hechendorf|Steinebach|We&#223;ling(Oberbay)|Neugilching|Gilching-Argelsried|Geisenbrunn|Germering-Unterpfaffenhofen|Harthaus|M&#252;nchen-Freiham|M&#252;nchen-Neuaubing|M&#252;nchen-Westkreuz"/>
    <dp pt="2107261705" pp="6" l="8" ppth="M&#252;nchen-Laim|M&#252;nchen Hirschgarten|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Hbf (tief)|M&#252;nchen Karlsplatz|M&#252;nchen Marienplatz|M&#252;nchen Isartor|M&#252;nchen Rosenheimer Platz|M&#252;nchen Ost|M&#252;nchen Leuchtenbergring|M&#252;nchen-Daglfing|M&#252;nchen-Englschalking|M&#252;nchen-Johanneskirchen|Unterf&#246;hring|Ismaning|Hallbergmoos|M&#252;nchen Flughafen Besucherpark|M&#252;nchen Flughafen Terminal"/>
  </s>
  <s id="-2535508527242359910-2107261734-2">
    <tl f="N" t="p" o="800734" c="RE" n="57154"/>
    <ar pt="2107261741" pp="10" l="8" ppth="M&#252;nchen Hbf"/>
    <dp pt="2107261742" pp="10" l="8" ppth="Mering|Mering-St Afra|Kissing|Augsburg-Hochzoll|Augsburg Haunstetterstra&#223;e|Augsburg Hbf|Augsburg-Oberhausen|Meitingen|Nordendorf|Mertingen Bahnhof|Donauw&#246;rth|Otting-Weilheim|Treuchtlingen"/>
  </s>
  <s id="2145835675191493643-2107261701-6">
    <tl f="S" t="p" o="800725" c="S" n="8875"/>
    <ar pt="2107261712" pp="5" l="8" ppth="Germering-Unterpfaffenhofen|Harthaus|M&#252;nchen-Freiham|M&#252;nchen-Neuaubing|M&#252;nchen-Westkreuz"/>
    <dp pt="2107261715" pp="5" l="8" ppth="M&#252;nchen-Laim|M&#252;nchen Hirschgarten|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Hbf (tief)|M&#252;nchen Karlsplatz|M&#252;nchen Marienplatz|M&#252;nchen Isartor|M&#252;nchen Rosenheimer Platz|M&#252;nchen Ost"/>
  </s>
  <s id="3428609904739917314-2107261734-2">
    <tl f="N" t="p" o="800734" c="RE" n="57232"/>
    <ar pt="2107261741" pp="10" l="89" ppth="M&#252;nchen Hbf"/>
    <dp pt="2107261742" pp="10" l="89" pde="Aalen Hbf" ppth="Mering|Mering-St Afra|Kissing|Augsburg-Hochzoll|Augsburg Haunstetterstra&#223;e|Augsburg Hbf"/>
  </s>
  <s id="-2207196734316120839-2107261702-8">
    <tl f="S" t="p" o="800725" c="S" n="8379"/>
    <ar pt="2107261720" pp="5" l="3" ppth="Maisach|Gernlinden|Esting|Olching|Gr&#246;benzell|M&#252;nchen-Lochhausen|M&#252;nchen-Langwied"/>
    <dp pt="2107261723" pp="5" l="3" ppth="M&#252;nchen-Laim|M&#252;nchen Hirschgarten|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Hbf (tief)|M&#252;nchen Karlsplatz|M&#252;nchen Marienplatz|M&#252;nchen Isartor|M&#252;nchen Rosenheimer Platz|M&#252;nchen Ost|M&#252;nchen St.Martin-Str.|M&#252;nchen-Giesing|M&#252;nchen-Fasangarten|Fasanenpark|Unterhaching|Taufkirchen|Furth(b Deisenhofen)|Deisenhofen"/>
  </s>
  <s id="-4489939964355886024-2107261700-2">
    <tl f="N" t="p" o="800734" c="RB" n="57152"/>
    <ar pt="2107261707" pp="10" l="87" ppth="M&#252;nchen Hbf"/>
    <dp pt="2107261707" pp="10" l="87" pde="N&#246;rdlingen" ppth="Mammendorf|Haspelmoor|Althegnenberg|Mering|Mering-St Afra|Kissing|Augsburg-Hochzoll|Augsburg Haunstetterstra&#223;e|Augsburg Hbf|Augsburg-Oberhausen|Gersthofen|Gablingen|Langweid(Lech)|Herbertshofen|Meitingen|Westendorf|Nordendorf|Mertingen Bahnhof|B&#228;umenheim|Donauw&#246;rth"/>
  </s>
  <s id="266527286084119102-2107261645-10">
    <tl f="N" t="p" o="800790" c="RB" n="59622"/>
    <ar pt="2107261752" pp="3" l="66" wings="5111774224362867641-2107261718" ppth="Kochel|Benediktbeuern|Bichl|Penzberg|Iffeldorf|Seeshaupt|Bernried|Tutzing|Starnberg"/>
    <dp pt="2107261753" pp="3" l="66" wings="5111774224362867641-2107261718" ppth="M&#252;nchen Hbf Gl.27-36"/>
  </s>
  <s id="-8932159143457955186-2107261732-2">
    <tl f="N" t="p" o="800790" c="RB" n="5433"/>
    <ar pt="2107261738" pp="4" l="6" wings="2005535188249917016-2107261732" ppth="M&#252;nchen Hbf Gl.27-36"/>
    <dp pt="2107261738" pp="4" l="6" wings="2005535188249917016-2107261732" pde="Seefeld in Tirol" ppth="Tutzing|Weilheim(Oberbay)|Huglfing|Uffing a Staffelsee|Murnau|Ohlstadt|Eschenlohe|Oberau|Farchant|Garmisch-Partenkirchen|Klais|Mittenwald"/>
  </s>
  <s id="-1428612075705409337-2107261700-2">
    <tl f="N" t="p" o="800734" c="RB" n="57052"/>
    <ar pt="2107261707" pp="10" l="86" wings="-4489939964355886024-2107261700" ppth="M&#252;nchen Hbf"/>
    <dp pt="2107261707" pp="10" l="86" wings="-4489939964355886024-2107261700" ppth="Mammendorf|Haspelmoor|Althegnenberg|Mering|Mering-St Afra|Kissing|Augsburg-Hochzoll|Augsburg Haunstetterstra&#223;e|Augsburg Hbf|Augsburg-Oberhausen|Neus&#228;&#223;|Westheim(Schwab)|Diedorf(Schwab)|Gessertshausen|Kutzenhausen|Dinkelscherben"/>
  </s>
  <s id="-4980676726219381127-2107261721-6">
    <tl f="S" t="p" o="800725" c="S" n="8877"/>
    <ar pt="2107261732" pp="5" l="8" ppth="Germering-Unterpfaffenhofen|Harthaus|M&#252;nchen-Freiham|M&#252;nchen-Neuaubing|M&#252;nchen-Westkreuz"/>
    <dp pt="2107261735" pp="5" l="8" ppth="M&#252;nchen-Laim|M&#252;nchen Hirschgarten|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Hbf (tief)|M&#252;nchen Karlsplatz|M&#252;nchen Marienplatz|M&#252;nchen Isartor|M&#252;nchen Rosenheimer Platz|M&#252;nchen Ost"/>
  </s>
  <s id="5749582172974013545-2107261645-13">
    <tl f="S" t="p" o="800725" c="S" n="6889"/>
    <ar pt="2107261722" pp="6" l="8" ppth="Herrsching|Seefeld-Hechendorf|Steinebach|We&#223;ling(Oberbay)|Neugilching|Gilching-Argelsried|Geisenbrunn|Germering-Unterpfaffenhofen|Harthaus|M&#252;nchen-Freiham|M&#252;nchen-Neuaubing|M&#252;nchen-Westkreuz"/>
    <dp pt="2107261725" pp="6" l="8" ppth="M&#252;nchen-Laim|M&#252;nchen Hirschgarten|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Hbf (tief)|M&#252;nchen Karlsplatz|M&#252;nchen Marienplatz|M&#252;nchen Isartor|M&#252;nchen Rosenheimer Platz|M&#252;nchen Ost|M&#252;nchen Leuchtenbergring|M&#252;nchen-Daglfing|M&#252;nchen-Englschalking|M&#252;nchen-Johanneskirchen|Unterf&#246;hring|Ismaning|Hallbergmoos|M&#252;nchen Flughafen Besucherpark|M&#252;nchen Flughafen Terminal"/>
  </s>
  <s id="8836113019682202018-2107261720-19">
    <tl f="S" t="p" o="800725" c="S" n="8382"/>
    <ar pt="2107261757" pp="8" l="3" ppth="Deisenhofen|Furth(b Deisenhofen)|Taufkirchen|Unterhaching|Fasanenpark|M&#252;nchen-Fasangarten|M&#252;nchen-Giesing|M&#252;nchen St.Martin-Str.|M&#252;nchen Ost|M&#252;nchen Rosenheimer Platz|M&#252;nchen Isartor|M&#252;nchen Marienplatz|M&#252;nchen Karlsplatz|M&#252;nchen Hbf (tief)|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hirschgarten|M&#252;nchen-Laim"/>
    <dp pt="2107261759" pp="8" l="3" ppth="M&#252;nchen-Langwied|M&#252;nchen-Lochhausen|Gr&#246;benzell|Olching|Esting|Gernlinden|Maisach"/>
  </s>
  <s id="3384671952599699995-2107261734-2">
    <tl f="N" t="p" o="800734" c="RE" n="57054"/>
    <ar pt="2107261741" pp="10" l="9" wings="-2535508527242359910-2107261734|3428609904739917314-2107261734" ppth="M&#252;nchen Hbf"/>
    <dp pt="2107261742" pp="10" l="9" wings="-2535508527242359910-2107261734|3428609904739917314-2107261734" ppth="Mering|Mering-St Afra|Kissing|Augsburg-Hochzoll|Augsburg Haunstetterstra&#223;e|Augsburg Hbf|Augsburg-Oberhausen|Neus&#228;&#223;|Westheim(Schwab)|Diedorf(Schwab)|Gessertshausen|Kutzenhausen|Dinkelscherben|Freihalden|Jettingen|Burgau(Schwab)|Mindelaltheim|Offingen|G&#252;nzburg|Leipheim|Nersingen|Neu-Ulm|Ulm Hbf"/>
  </s>
  <s id="-4835907071584988421-2107261704-12">
    <tl f="S" t="p" o="800725" c="S" n="6689"/>
    <ar pt="2107261737" pp="6" l="6" ppth="Tutzing|Feldafing|Possenhofen|Starnberg|Starnberg Nord|Gauting|Stockdorf|Planegg|Gr&#228;felfing|Lochham|M&#252;nchen-Westkreuz"/>
    <dp pt="2107261738" pp="6" l="6" ppth="M&#252;nchen-Laim|M&#252;nchen Hirschgarten|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Hbf (tief)|M&#252;nchen Karlsplatz|M&#252;nchen Marienplatz|M&#252;nchen Isartor|M&#252;nchen Rosenheimer Platz|M&#252;nchen Ost|M&#252;nchen Leuchtenbergring|M&#252;nchen-Berg am Laim|M&#252;nchen-Trudering|Gronsdorf|Haar|Vaterstetten|Baldham|Zorneding|Eglharting|Kirchseeon|Grafing Bahnhof|Grafing Stadt|Ebersberg(Oberbay)"/>
  </s>
  <s id="-2167902259927965041-2107261709-2">
    <tl f="N" t="p" o="800790" c="RE" n="4693"/>
    <ar pt="2107261714" pp="4" l="61" ppth="M&#252;nchen Hbf Gl.27-36"/>
    <dp pt="2107261715" pp="4" l="61" ppth="Weilheim(Oberbay)|Murnau|Garmisch-Partenkirchen|Klais|Mittenwald"/>
  </s>
  <s id="-8866688238682161636-2107261636-22">
    <tl f="S" t="p" o="800725" c="S" n="6384"/>
    <ar pt="2107261727" pp="8" l="3" ppth="Holzkirchen|Otterfing|Sauerlach|Deisenhofen|Furth(b Deisenhofen)|Taufkirchen|Unterhaching|Fasanenpark|M&#252;nchen-Fasangarten|M&#252;nchen-Giesing|M&#252;nchen St.Martin-Str.|M&#252;nchen Ost|M&#252;nchen Rosenheimer Platz|M&#252;nchen Isartor|M&#252;nchen Marienplatz|M&#252;nchen Karlsplatz|M&#252;nchen Hbf (tief)|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hirschgarten|M&#252;nchen-Laim"/>
    <dp pt="2107261729" pp="8" l="3" ppth="M&#252;nchen-Langwied|M&#252;nchen-Lochhausen|Gr&#246;benzell|Olching|Esting|Gernlinden|Maisach|Malching(Oberbay)|Mammendorf"/>
  </s>
  <s id="5024821168930928586-2107261622-24">
    <tl f="S" t="p" o="800725" c="S" n="6684"/>
    <ar pt="2107261721" pp="7" l="6" ppth="Ebersberg(Oberbay)|Grafing Stadt|Grafing Bahnhof|Kirchseeon|Eglharting|Zorneding|Baldham|Vaterstetten|Haar|Gronsdorf|M&#252;nchen-Trudering|M&#252;nchen-Berg am Laim|M&#252;nchen Leuchtenbergring|M&#252;nchen Ost|M&#252;nchen Rosenheimer Platz|M&#252;nchen Isartor|M&#252;nchen Marienplatz|M&#252;nchen Karlsplatz|M&#252;nchen Hbf (tief)|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hirschgarten|M&#252;nchen-Laim"/>
    <dp pt="2107261723" pp="7" l="6" ppth="M&#252;nchen-Westkreuz|Lochham|Gr&#228;felfing|Planegg|Stockdorf|Gauting|Starnberg Nord|Starnberg|Possenhofen|Feldafing|Tutzing"/>
  </s>
  <s id="8084569115421632238-2107261705-13">
    <tl f="S" t="p" o="800725" c="S" n="6891"/>
    <ar pt="2107261742" pp="6" l="8" ppth="Herrsching|Seefeld-Hechendorf|Steinebach|We&#223;ling(Oberbay)|Neugilching|Gilching-Argelsried|Geisenbrunn|Germering-Unterpfaffenhofen|Harthaus|M&#252;nchen-Freiham|M&#252;nchen-Neuaubing|M&#252;nchen-Westkreuz"/>
    <dp pt="2107261745" pp="6" l="8" ppth="M&#252;nchen-Laim|M&#252;nchen Hirschgarten|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Hbf (tief)|M&#252;nchen Karlsplatz|M&#252;nchen Marienplatz|M&#252;nchen Isartor|M&#252;nchen Rosenheimer Platz|M&#252;nchen Ost|M&#252;nchen Leuchtenbergring|M&#252;nchen-Daglfing|M&#252;nchen-Englschalking|M&#252;nchen-Johanneskirchen|Unterf&#246;hring|Ismaning|Hallbergmoos|M&#252;nchen Flughafen Besucherpark|M&#252;nchen Flughafen Terminal"/>
  </s>
  <s id="9070758904460231831-2107261603-24">
    <tl f="S" t="p" o="800725" c="S" n="6682"/>
    <ar pt="2107261701" pp="7" l="6" ppth="Ebersberg(Oberbay)|Grafing Stadt|Grafing Bahnhof|Kirchseeon|Eglharting|Zorneding|Baldham|Vaterstetten|Haar|Gronsdorf|M&#252;nchen-Trudering|M&#252;nchen-Berg am Laim|M&#252;nchen Leuchtenbergring|M&#252;nchen Ost|M&#252;nchen Rosenheimer Platz|M&#252;nchen Isartor|M&#252;nchen Marienplatz|M&#252;nchen Karlsplatz|M&#252;nchen Hbf (tief)|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hirschgarten|M&#252;nchen-Laim"/>
    <dp pt="2107261703" pp="7" l="6" ppth="M&#252;nchen-Westkreuz|Lochham|Gr&#228;felfing|Planegg|Stockdorf|Gauting|Starnberg Nord|Starnberg|Possenhofen|Feldafing|Tutzing"/>
  </s>
  <s id="-2806660218089745291-2107261619-18">
    <tl f="N" t="p" o="800734" c="RB" n="57051"/>
    <ar pt="2107261739" pp="9" l="87" wings="-7476534183040552185-2107261629" ppth="Donauw&#246;rth|B&#228;umenheim|Mertingen Bahnhof|Nordendorf|Westendorf|Meitingen|Herbertshofen|Langweid(Lech)|Gablingen|Gersthofen|Augsburg-Oberhausen|Augsburg Hbf|Augsburg Haunstetterstra&#223;e|Augsburg-Hochzoll|Kissing|Mering-St Afra|Mering"/>
    <dp pt="2107261740" pp="9" l="87" wings="-7476534183040552185-2107261629" ppth="M&#252;nchen Hbf"/>
  </s>
  <s id="4028565785074885789-2107261604-14">
    <tl f="D" t="p" o="Y8" c="BRB" n="62716"/>
    <ar pt="2107261757" pp="3" l="RB68" ppth="F&#252;ssen|Weizern-Hopferau|Seeg|Lengenwang|Leuterschach|Marktoberdorf Schule|Marktoberdorf|Ebenhofen|Biessenhofen|Kaufbeuren|Buchloe|Kaufering|Geltendorf"/>
    <dp pt="2107261758" pp="3" l="RB68" ppth="M&#252;nchen Hbf Gl.27-36"/>
  </s>
  <s id="4034020218161374000-2107261704-20">
    <tl f="S" t="p" o="800725" c="S" n="6886"/>
    <ar pt="2107261755" pp="7" l="8" ppth="M&#252;nchen Flughafen Terminal|M&#252;nchen Flughafen Besucherpark|Hallbergmoos|Ismaning|Unterf&#246;hring|M&#252;nchen-Johanneskirchen|M&#252;nchen-Englschalking|M&#252;nchen-Daglfing|M&#252;nchen Leuchtenbergring|M&#252;nchen Ost|M&#252;nchen Rosenheimer Platz|M&#252;nchen Isartor|M&#252;nchen Marienplatz|M&#252;nchen Karlsplatz|M&#252;nchen Hbf (tief)|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hirschgarten|M&#252;nchen-Laim"/>
    <dp pt="2107261757" pp="7" l="8" ppth="M&#252;nchen-Westkreuz|M&#252;nchen-Neuaubing|M&#252;nchen-Freiham|Harthaus|Germering-Unterpfaffenhofen|Geisenbrunn|Gilching-Argelsried|Neugilching|We&#223;ling(Oberbay)|Steinebach|Seefeld-Hechendorf|Herrsching"/>
  </s>
  <s id="8137999038578721509-2107261654-11">
    <tl f="S" t="p" o="800725" c="S" n="6489"/>
    <ar pt="2107261726" pp="5" l="4" ppth="Geltendorf|T&#252;rkenfeld|Grafrath|Sch&#246;ngeising|Buchenau(Oberbay)|F&#252;rstenfeldbruck|Eichenau(Oberbay)|Puchheim|M&#252;nchen-Aubing|M&#252;nchen Leienfelsstr."/>
    <dp pt="2107261729" pp="5" l="4" ppth="M&#252;nchen-Laim|M&#252;nchen Hirschgarten|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Hbf (tief)|M&#252;nchen Karlsplatz|M&#252;nchen Marienplatz|M&#252;nchen Isartor|M&#252;nchen Rosenheimer Platz|M&#252;nchen Ost|M&#252;nchen Leuchtenbergring|M&#252;nchen-Berg am Laim|M&#252;nchen-Trudering|Gronsdorf|Haar|Vaterstetten|Baldham|Zorneding|Eglharting|Kirchseeon|Grafing Bahnhof"/>
  </s>
  <s id="-4125087315318930263-2107261722-8">
    <tl f="S" t="p" o="800725" c="S" n="8381"/>
    <ar pt="2107261740" pp="5" l="3" ppth="Maisach|Gernlinden|Esting|Olching|Gr&#246;benzell|M&#252;nchen-Lochhausen|M&#252;nchen-Langwied"/>
    <dp pt="2107261743" pp="5" l="3" ppth="M&#252;nchen-Laim|M&#252;nchen Hirschgarten|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Hbf (tief)|M&#252;nchen Karlsplatz|M&#252;nchen Marienplatz|M&#252;nchen Isartor|M&#252;nchen Rosenheimer Platz|M&#252;nchen Ost|M&#252;nchen St.Martin-Str.|M&#252;nchen-Giesing|M&#252;nchen-Fasangarten|Fasanenpark|Unterhaching|Taufkirchen|Furth(b Deisenhofen)|Deisenhofen"/>
  </s>
  <s id="5111774224362867641-2107261718-4">
    <tl f="N" t="p" o="800790" c="RB" n="59528"/>
    <ar pt="2107261752" pp="3" l="65" ppth="Weilheim(Oberbay)|Tutzing|Starnberg"/>
    <dp pt="2107261753" pp="3" l="65" ppth="M&#252;nchen Hbf Gl.27-36"/>
  </s>
  <s id="-262906672946942861-2107261607-9">
    <tl f="N" t="p" o="8007D5" c="RE" n="57413"/>
    <ar pt="2107261733" pp="3" l="72" ppth="Memmingen|Sontheim(Schwab)|Stetten(Schwab)|Mindelheim|T&#252;rkheim(Bay)Bf|Buchloe|Kaufering|Geltendorf"/>
    <dp pt="2107261734" pp="3" l="72" ppth="M&#252;nchen Hbf Gl.27-36"/>
  </s>
  <s id="4226778102181650539-2107261729-8">
    <tl f="S" t="p" o="800725" c="S" n="8168"/>
    <ar pt="2107261749" pp="4" l="20" ppth="H&#246;llriegelskreuth|Pullach|Gro&#223;hesselohe Isartalbf|M&#252;nchen-Solln|M&#252;nchen Siemenswerke|M&#252;nchen-Mittersendling|M&#252;nchen Heimeranplatz"/>
    <dp pt="2107261750" pp="4" l="20" ppth="M&#252;nchen Leienfelsstr.|M&#252;nchen-Aubing|Puchheim|Eichenau(Oberbay)|F&#252;rstenfeldbruck|Buchenau(Oberbay)|Sch&#246;ngeising|Grafrath"/>
  </s>
  <s id="1229169027606729662-2107261700-19">
    <tl f="S" t="p" o="800725" c="S" n="8380"/>
    <ar pt="2107261737" pp="8" l="3" ppth="Deisenhofen|Furth(b Deisenhofen)|Taufkirchen|Unterhaching|Fasanenpark|M&#252;nchen-Fasangarten|M&#252;nchen-Giesing|M&#252;nchen St.Martin-Str.|M&#252;nchen Ost|M&#252;nchen Rosenheimer Platz|M&#252;nchen Isartor|M&#252;nchen Marienplatz|M&#252;nchen Karlsplatz|M&#252;nchen Hbf (tief)|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hirschgarten|M&#252;nchen-Laim"/>
    <dp pt="2107261739" pp="8" l="3" ppth="M&#252;nchen-Langwied|M&#252;nchen-Lochhausen|Gr&#246;benzell|Olching|Esting|Gernlinden|Maisach"/>
  </s>
  <s id="5962363533785085714-2107261656-22">
    <tl f="S" t="p" o="800725" c="S" n="6386"/>
    <ar pt="2107261747" pp="8" l="3" ppth="Holzkirchen|Otterfing|Sauerlach|Deisenhofen|Furth(b Deisenhofen)|Taufkirchen|Unterhaching|Fasanenpark|M&#252;nchen-Fasangarten|M&#252;nchen-Giesing|M&#252;nchen St.Martin-Str.|M&#252;nchen Ost|M&#252;nchen Rosenheimer Platz|M&#252;nchen Isartor|M&#252;nchen Marienplatz|M&#252;nchen Karlsplatz|M&#252;nchen Hbf (tief)|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hirschgarten|M&#252;nchen-Laim"/>
    <dp pt="2107261749" pp="8" l="3" ppth="M&#252;nchen-Langwied|M&#252;nchen-Lochhausen|Gr&#246;benzell|Olching|Esting|Gernlinden|Maisach|Malching(Oberbay)|Mammendorf"/>
  </s>
  <s id="-4851507660197540317-2107261713-2">
    <tl f="N" t="p" o="800734" c="RB" n="57094"/>
    <ar pt="2107261720" pp="10" l="86" ppth="M&#252;nchen Hbf"/>
    <dp pt="2107261721" pp="10" l="86" ppth="Mering|Augsburg-Hochzoll|Augsburg Haunstetterstra&#223;e|Augsburg Hbf|Augsburg-Oberhausen|Neus&#228;&#223;|Westheim(Schwab)|Diedorf(Schwab)|Gessertshausen"/>
  </s>
  <s id="1056662216579043475-2107261623-20">
    <tl f="S" t="p" o="800725" c="S" n="6882"/>
    <ar pt="2107261715" pp="7" l="8" ppth="M&#252;nchen Flughafen Terminal|M&#252;nchen Flughafen Besucherpark|Hallbergmoos|Ismaning|Unterf&#246;hring|M&#252;nchen-Johanneskirchen|M&#252;nchen-Englschalking|M&#252;nchen-Daglfing|M&#252;nchen Leuchtenbergring|M&#252;nchen Ost|M&#252;nchen Rosenheimer Platz|M&#252;nchen Isartor|M&#252;nchen Marienplatz|M&#252;nchen Karlsplatz|M&#252;nchen Hbf (tief)|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hirschgarten|M&#252;nchen-Laim"/>
    <dp pt="2107261717" pp="7" l="8" ppth="M&#252;nchen-Westkreuz|M&#252;nchen-Neuaubing|M&#252;nchen-Freiham|Harthaus|Germering-Unterpfaffenhofen|Geisenbrunn|Gilching-Argelsried|Neugilching|We&#223;ling(Oberbay)|Steinebach|Seefeld-Hechendorf|Herrsching"/>
  </s>
  <s id="-3871388708284631831-2107261714-11">
    <tl f="S" t="p" o="800725" c="S" n="6491"/>
    <ar pt="2107261746" pp="5" l="4" ppth="Geltendorf|T&#252;rkenfeld|Grafrath|Sch&#246;ngeising|Buchenau(Oberbay)|F&#252;rstenfeldbruck|Eichenau(Oberbay)|Puchheim|M&#252;nchen-Aubing|M&#252;nchen Leienfelsstr."/>
    <dp pt="2107261749" pp="5" l="4" ppth="M&#252;nchen-Laim|M&#252;nchen Hirschgarten|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Hbf (tief)|M&#252;nchen Karlsplatz|M&#252;nchen Marienplatz|M&#252;nchen Isartor|M&#252;nchen Rosenheimer Platz|M&#252;nchen Ost|M&#252;nchen Leuchtenbergring|M&#252;nchen-Berg am Laim|M&#252;nchen-Trudering|Gronsdorf|Haar|Vaterstetten|Baldham|Zorneding|Eglharting|Kirchseeon|Grafing Bahnhof"/>
  </s>
  <s id="7495263095270404585-2107261642-8">
    <tl f="S" t="p" o="800725" c="S" n="8377"/>
    <ar pt="2107261700" pp="5" l="3" ppth="Maisach|Gernlinden|Esting|Olching|Gr&#246;benzell|M&#252;nchen-Lochhausen|M&#252;nchen-Langwied"/>
    <dp pt="2107261703" pp="5" l="3" ppth="M&#252;nchen-Laim|M&#252;nchen Hirschgarten|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Hbf (tief)|M&#252;nchen Karlsplatz|M&#252;nchen Marienplatz|M&#252;nchen Isartor|M&#252;nchen Rosenheimer Platz|M&#252;nchen Ost|M&#252;nchen St.Martin-Str.|M&#252;nchen-Giesing|M&#252;nchen-Fasangarten|Fasanenpark|Unterhaching|Taufkirchen|Furth(b Deisenhofen)|Deisenhofen"/>
  </s>
  <s id="2922258149668511998-2107261641-22">
    <tl f="S" t="p" o="800725" c="S" n="6484"/>
    <ar pt="2107261731" pp="8" l="4" ppth="Grafing Bahnhof|Kirchseeon|Eglharting|Zorneding|Baldham|Vaterstetten|Haar|Gronsdorf|M&#252;nchen-Trudering|M&#252;nchen-Berg am Laim|M&#252;nchen Leuchtenbergring|M&#252;nchen Ost|M&#252;nchen Rosenheimer Platz|M&#252;nchen Isartor|M&#252;nchen Marienplatz|M&#252;nchen Karlsplatz|M&#252;nchen Hbf (tief)|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hirschgarten|M&#252;nchen-Laim"/>
    <dp pt="2107261733" pp="8" l="4" ppth="M&#252;nchen Leienfelsstr.|M&#252;nchen-Aubing|Puchheim|Eichenau(Oberbay)|F&#252;rstenfeldbruck|Buchenau(Oberbay)|Sch&#246;ngeising|Grafrath|T&#252;rkenfeld|Geltendorf"/>
  </s>
  <s id="-6928239653945243831-2107261644-20">
    <tl f="S" t="p" o="800725" c="S" n="6884"/>
    <ar pt="2107261735" pp="7" l="8" ppth="M&#252;nchen Flughafen Terminal|M&#252;nchen Flughafen Besucherpark|Hallbergmoos|Ismaning|Unterf&#246;hring|M&#252;nchen-Johanneskirchen|M&#252;nchen-Englschalking|M&#252;nchen-Daglfing|M&#252;nchen Leuchtenbergring|M&#252;nchen Ost|M&#252;nchen Rosenheimer Platz|M&#252;nchen Isartor|M&#252;nchen Marienplatz|M&#252;nchen Karlsplatz|M&#252;nchen Hbf (tief)|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hirschgarten|M&#252;nchen-Laim"/>
    <dp pt="2107261737" pp="7" l="8" ppth="M&#252;nchen-Westkreuz|M&#252;nchen-Neuaubing|M&#252;nchen-Freiham|Harthaus|Germering-Unterpfaffenhofen|Geisenbrunn|Gilching-Argelsried|Neugilching|We&#223;ling(Oberbay)|Steinebach|Seefeld-Hechendorf|Herrsching"/>
  </s>
  <s id="-4663732027095622670-2107261607-11">
    <tl f="N" t="p" o="800790" c="RB" n="59460"/>
    <ar pt="2107261719" pp="3" l="6" ppth="Garmisch-Partenkirchen|Farchant|Oberau|Eschenlohe|Ohlstadt|Murnau|Uffing a Staffelsee|Huglfing|Weilheim(Oberbay)|Tutzing"/>
    <dp pt="2107261720" pp="3" l="6" ppth="M&#252;nchen Hbf Gl.27-36"/>
  </s>
  <s id="-8982097396576845829-2107261751-2">
    <tl f="D" t="p" o="Y8" c="BRB" n="62719"/>
    <ar pt="2107261758" pp="4" l="RB68" ppth="M&#252;nchen Hbf Gl.27-36"/>
    <dp pt="2107261759" pp="4" l="RB68" ppth="Geltendorf|Kaufering|Buchloe|Kaufbeuren|Biessenhofen|Ebenhofen|Marktoberdorf|Marktoberdorf Schule|Leuterschach|Lengenwang|Seeg|Weizern-Hopferau|F&#252;ssen"/>
  </s>
  <s id="-7724390263517347378-2107261705-2">
    <tl f="F" t="p" o="80" c="ICE" n="702"/>
    <ar pt="2107261712" pp="10" hi="1" ppth="M&#252;nchen Hbf"/>
    <dp pt="2107261714" pp="10" ppth="Augsburg Hbf|Donauw&#246;rth|Treuchtlingen|N&#252;rnberg Hbf|Bamberg|Erfurt Hbf|Halle(Saale)Hbf|Bitterfeld|Berlin S&#252;dkreuz|Berlin Hbf (tief)|Berlin Gesundbrunnen"/>
  </s>
  <s id="-8830259945326434577-2107261536-8">
    <tl f="N" t="p" o="8007D4" c="RE" n="57595"/>
    <ar pt="2107261709" pp="3" l="74" ppth="Kempten(Allg&#228;u)Hbf|G&#252;nzach|Biessenhofen|Kaufbeuren|Buchloe|Kaufering|Geltendorf"/>
    <dp pt="2107261710" pp="3" l="74" ppth="M&#252;nchen Hbf Gl.27-36"/>
  </s>
  <s id="-5145428150435837672-2107261759-1">
    <tl f="S" t="p" o="800725" c="S" n="8169"/>
    <dp pt="2107261759" pp="2" l="20" ppth="M&#252;nchen Heimeranplatz|M&#252;nchen-Mittersendling|M&#252;nchen Siemenswerke|M&#252;nchen-Solln|Gro&#223;hesselohe Isartalbf|Pullach|H&#246;llriegelskreuth"/>
  </s>
  <s id="-4666246263723012968-2107261523-24">
    <tl f="N" t="p" o="800734" c="RE" n="57049"/>
    <ar pt="2107261713" pp="9" l="9" ppth="Ulm Hbf|Neu-Ulm|Nersingen|Leipheim|G&#252;nzburg|Offingen|Mindelaltheim|Burgau(Schwab)|Jettingen|Freihalden|Dinkelscherben|Kutzenhausen|Gessertshausen|Diedorf(Schwab)|Westheim(Schwab)|Neus&#228;&#223;|Augsburg-Oberhausen|Augsburg Hbf|Augsburg Haunstetterstra&#223;e|Augsburg-Hochzoll|Kissing|Mering-St Afra|Mering"/>
    <dp pt="2107261714" pp="9" l="9" ppth="M&#252;nchen Hbf"/>
  </s>
  <s id="-5051211802840884631-2107261716-6">
    <tl f="N" t="p" o="800734" c="RE" n="57089"/>
    <ar pt="2107261754" pp="9" l="9" ppth="Augsburg Hbf|Mering|Althegnenberg|Haspelmoor|Mammendorf"/>
    <dp pt="2107261755" pp="9" l="9" ppth="M&#252;nchen Hbf"/>
  </s>
  <s id="8266786444049700149-2107261644-10">
    <tl f="S" t="p" o="800725" c="S" n="6387"/>
    <ar pt="2107261710" pp="6" l="3" ppth="Mammendorf|Malching(Oberbay)|Maisach|Gernlinden|Esting|Olching|Gr&#246;benzell|M&#252;nchen-Lochhausen|M&#252;nchen-Langwied"/>
    <dp pt="2107261713" pp="6" l="3" ppth="M&#252;nchen-Laim|M&#252;nchen Hirschgarten|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Hbf (tief)|M&#252;nchen Karlsplatz|M&#252;nchen Marienplatz|M&#252;nchen Isartor|M&#252;nchen Rosenheimer Platz|M&#252;nchen Ost|M&#252;nchen St.Martin-Str.|M&#252;nchen-Giesing|M&#252;nchen-Fasangarten|Fasanenpark|Unterhaching|Taufkirchen|Furth(b Deisenhofen)|Deisenhofen|Sauerlach|Otterfing|Holzkirchen"/>
  </s>
  <s id="-7740791581162846662-2107261727-11">
    <tl f="S" t="p" o="800725" c="S" n="8876"/>
    <ar pt="2107261745" pp="7" l="8" ppth="M&#252;nchen Ost|M&#252;nchen Rosenheimer Platz|M&#252;nchen Isartor|M&#252;nchen Marienplatz|M&#252;nchen Karlsplatz|M&#252;nchen Hbf (tief)|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hirschgarten|M&#252;nchen-Laim"/>
    <dp pt="2107261746" pp="7" l="8" ppth="M&#252;nchen-Westkreuz|M&#252;nchen-Neuaubing|M&#252;nchen-Freiham|Harthaus|Germering-Unterpfaffenhofen"/>
  </s>
  <s id="-7476534183040552185-2107261629-14">
    <tl f="N" t="p" o="800734" c="RB" n="57151"/>
    <ar pt="2107261739" pp="9" l="86" ppth="Dinkelscherben|Kutzenhausen|Gessertshausen|Diedorf(Schwab)|Westheim(Schwab)|Neus&#228;&#223;|Augsburg-Oberhausen|Augsburg Hbf|Augsburg Haunstetterstra&#223;e|Augsburg-Hochzoll|Kissing|Mering-St Afra|Mering"/>
    <dp pt="2107261740" pp="9" l="86" ppth="M&#252;nchen Hbf"/>
  </s>
  <s id="-8625914546178955863-2107261704-10">
    <tl f="S" t="p" o="800725" c="S" n="6389"/>
    <ar pt="2107261730" pp="6" l="3" ppth="Mammendorf|Malching(Oberbay)|Maisach|Gernlinden|Esting|Olching|Gr&#246;benzell|M&#252;nchen-Lochhausen|M&#252;nchen-Langwied"/>
    <dp pt="2107261733" pp="6" l="3" ppth="M&#252;nchen-Laim|M&#252;nchen Hirschgarten|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Hbf (tief)|M&#252;nchen Karlsplatz|M&#252;nchen Marienplatz|M&#252;nchen Isartor|M&#252;nchen Rosenheimer Platz|M&#252;nchen Ost|M&#252;nchen St.Martin-Str.|M&#252;nchen-Giesing|M&#252;nchen-Fasangarten|Fasanenpark|Unterhaching|Taufkirchen|Furth(b Deisenhofen)|Deisenhofen|Sauerlach|Otterfing|Holzkirchen"/>
  </s>
  <s id="4122872863892302244-2107261701-22">
    <tl f="S" t="p" o="800725" c="S" n="6486"/>
    <ar pt="2107261751" pp="8" l="4" ppth="Grafing Bahnhof|Kirchseeon|Eglharting|Zorneding|Baldham|Vaterstetten|Haar|Gronsdorf|M&#252;nchen-Trudering|M&#252;nchen-Berg am Laim|M&#252;nchen Leuchtenbergring|M&#252;nchen Ost|M&#252;nchen Rosenheimer Platz|M&#252;nchen Isartor|M&#252;nchen Marienplatz|M&#252;nchen Karlsplatz|M&#252;nchen Hbf (tief)|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hirschgarten|M&#252;nchen-Laim"/>
    <dp pt="2107261753" pp="8" l="4" ppth="M&#252;nchen Leienfelsstr.|M&#252;nchen-Aubing|Puchheim|Eichenau(Oberbay)|F&#252;rstenfeldbruck|Buchenau(Oberbay)|Sch&#246;ngeising|Grafrath|T&#252;rkenfeld|Geltendorf"/>
  </s>
  <s id="-7878031020097350412-2107261630-19">
    <tl f="S" t="p" o="800725" c="S" n="6482"/>
    <ar pt="2107261711" pp="8" l="4" ppth="Zorneding|Baldham|Vaterstetten|Haar|Gronsdorf|M&#252;nchen-Trudering|M&#252;nchen-Berg am Laim|M&#252;nchen Leuchtenbergring|M&#252;nchen Ost|M&#252;nchen Rosenheimer Platz|M&#252;nchen Isartor|M&#252;nchen Marienplatz|M&#252;nchen Karlsplatz|M&#252;nchen Hbf (tief)|M&#252;nchen Hackerbr&#252;cke|M&#252;nchen Donnersbergerbr&#252;cke|M&#252;nchen Hirschgarten|M&#252;nchen-Laim"/>
    <dp pt="2107261713" pp="8" l="4" ppth="M&#252;nchen Leienfelsstr.|M&#252;nchen-Aubing|Puchheim|Eichenau(Oberbay)|F&#252;rstenfeldbruck|Buchenau(Oberbay)|Sch&#246;ngeising|Grafrath|T&#252;rkenfeld|Geltendorf"/>
  </s>
</timetable>'''