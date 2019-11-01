from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql import functions as F

def create_spark_session(app_name):
  spark=SparkSession.builder.appName(app_name).config("hive.exec.dynamic.partition.mode","nonstrict").config("hive.exec.dynamic.partition","true").
    enableHiveSupport().getOrCreate()
  spark.conf.set("mapred.input.dir.recursive","true")
  spark.conf.set("mapreduce.input.fileinputformat.input.dir.recursive","true")
  return spark
def close_spark_session:
  spark.stop()
def main():
   spark=create_spark_session("hive upserts")
   upsert_sql="select * from upsert_tbl"
   read_incr_df=spark.sql(upsert_sql)
   curr_minus_incr_sql="select * from curr left join upsert_tbl u on curr.pk=u.pk where u.pk IS NULL"
   merge_df=curr_minus_incr_sql.union(read_incr_df)
   merge_df.write.save("hdfs path")
   '''writing to temp view'''
   merge_df.createOrReplaceTempView("merge_tbl")
   '''print schema to log file'''
   merge_df_schema=merge_df._jdf.schema().treeString()
   '''select fields from df'''
   merge_df.select('id','name').show(n=1)
   merge_df.count()
