'''
To run: spark-submit --packages org.mongodb.spark:mongo-spark-connector_2.11:2.2.3 SparkJob3.py
'''

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("SQL Analysis TEST") \
    .config("spark.mongodb.input.uri", "mongodb://127.0.0.1/clusterdb.flinkStreamingResult") \
    .config("spark.mongodb.output.uri", "mongodb://127.0.0.1/clusterdb.sparkBatchResultJob3") \
    .getOrCreate()


df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()

df.createOrReplaceTempView("logs")
df.printSchema()

user_count = df.groupBy('name')\
    .count()\
    .sort('count',ascending=False)

user_count.show()

user_count.write \
    .format("com.mongodb.spark.sql.DefaultSource") \
    .mode("overwrite") \
    .save();
