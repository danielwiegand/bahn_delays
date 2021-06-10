
from pyspark import SparkContext
sc = SparkContext("local", "First App")

rdd = sc.parallelize(range(10))

rdd.count()

rdd.take(1)