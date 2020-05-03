from pyspark.sql import SparkSession,DataFrame,functions as F #check this
#from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import desc,lit
#from pyspark.sql import DataFrame
spark=SparkSession.builder.app_name("Temp").getOrCreate()
df1=spark.sql("select * from a")
#reading from hive table
df2=spark.sql("select * from b")
#reading from file
df3=spark.read.parquet("s3path of hive table location")
print("src cnt is "+str(df3.count()))


df1_row_number_as_one=df1.withColumn("rownum",
                               F.row_number().over(Window.partitionby("id","name").orderBy(desc("dateentered"),desc("dateupd"))))
df1_row_number_as_one.printSchema() #should see new field rownum in the schema
df1_filter_row_number_as_one=df1_row_number_as_one.filter("rownum==1").drop("rownum") #removing rownum filed from df
df1_filter_row_number_as_one.count() #get count
df1_row_number_as_one.show() #displays first few records
df1_row_number_as_one.registerTempTable("student")
df1_row_number_as_one.head(7)
df1_row_number_as_one.first()
df1_row_number_as_one.take(2) #displays first 2 records
df1_row_number_as_one.distinct().count() #gets records count with out duplicates lets say id,name 1,'abc',2,'xyz',2,'xyz' return 2 incase of count return 3
