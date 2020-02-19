from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
app_name="upd_fields"
#mutilpy hive accounts table amt,adj_amt field with 100 for few existing records based on the date
spark=SparkSession.builder.app_name("Temp").getOrCreate()
src_df=spark.read.parquet("s3path of hive table location")
print("src cnt is "+str(src_df.count()))

#don't update for date 01-01-2020
raw_df=src_df.filter(src_df.run_date=="2020-01-01")
to_be_upd_df=src_df.filter(src_df.run_date!="2020-01-01")

ra2_df.count
upd_df.count 

upd_df=to_be_upd_df.withColumn("amt",to_be_upd_df["amt"]*100).withColumn("adj_amt",to_be_upd_df["adj_amt"]*100)

#union raw df with upd and overwrite hive input path after validations
dfs_list=[src_df,upd_df] #can add n df's
result_df=reduce(DataFrame.unionAll,dfs_list)
result_df.count()

result_df.write.format("parquet").mode("overwrite").save("s3file")



