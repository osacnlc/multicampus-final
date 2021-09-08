from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

spark = SparkSession \
    .builder \
    .appName("emergencyMsg") \
    .getOrCreate()

MsgSchema = spark.read.format('json').load('/home/lab11/emergency/20210903105648EmergencyMsg20210903.json').schema
MsgDf = spark.readStream.schema(MsgSchema).json('/home/lab11/emergency/*.json')
df_msg = MsgDf.select(explode(MsgDf.DisasterMsg.row).alias("emergency")).select('emergency.*')
df_msg.coalesce(1).writeStream.format('json') \
    .option("checkpointLocation", "/home/lab11/emergency_check") \
    .option("path", "/home/lab11/emergency_msg") \
    .trigger(processingTime='7200 seconds') \
    .start().awaitTermination()