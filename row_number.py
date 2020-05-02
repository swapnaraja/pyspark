from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
spark=SparkSession.builder.app_name("Temp").getOrCreate()
df1=spark.sql("select * from a")
#reading from hive table
df2=spark.sql("select * from b")
#reading from file
df3=spark.read.parquet("s3path of hive table location")
print("src cnt is "+str(df3.count()))
