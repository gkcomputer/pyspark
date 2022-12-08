from pyspark.sql import SparkSession
from pyspark import SparkConf,SparkContext
from pyspark.sql.functions import concat, unix_timestamp, concat_ws, from_unixtime, col, to_timestamp


conf = SparkConf().setAppName("Spark_context")
sc = SparkContext.getOrCreate(conf=conf)

spark = SparkSession.builder.master("local").appName("Spark_session").getOrCreate()

# Df read
df = spark.read. \
    option("header",True). \
    option("inferSchema",True). \
    option("delimiter",","). \
    csv("D:/resources/covid_19_india.csv")
df.printSchema()

spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

# unix timestamp
df2 = df.select(concat_ws(" ","Date","Time").alias("time_stamp"))
df3 = df2.withColumn('time',to_timestamp('time_stamp','yyyy-MM-dd h:mm aa'))



df3.printSchema()
#df3= df2.withColumn("datetype_timestamp",to_timestamp(col("time_stamp"),"yyyy-MM-dd HH mm ss aa"))








#df3 = df2.select(unix_timestamp("time_stamp"))
#df3 = df2.withColumn("time_stamp",to_timestamp("time_stamp"))
#df3 =df2.withColumn("unixtime",from_unixtime(unix_timestamp(col('time_stamp'), "yyyy-MM-dd'T'hh:mm:ss aa"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))



df3.show(2,truncate=False)


