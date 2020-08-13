from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import from_unixtime
from pyspark.sql.functions import regexp_extract

#regex to clean file name
#regex_str = "[\/]([^\/]+[\/][^\/]+)$"
regex_str = "[\/]([^\/]+)$"

#First dataframe loads the "data" directory
df = spark.read.json("data")

#df adds a column filename on it.
#df adds a column click_time with the timestamp converted to date - for debugging purposes initially.
# adds column putting the timestamp on 10 digit unix
df = df.withColumn("filename", input_file_name()) \
       .withColumn('click_time',from_unixtime((df['device_sent_timestamp'])/1000)) \
       .withColumn('unix_ts',(df['device_sent_timestamp'])/1000)

df = df.withColumn("file", regexp_extract("filename",regex_str,1))

#test outputs
#df.select("anonymous_id","unix_ts","click_time").show()
#df.printSchema()
#df.select("anonymous_id","t3").show()

w1 = Window.partitionBy('anonymous_id').orderBy('unix_ts')
df_1 = df.withColumn('ts_diff',F.coalesce((df['unix_ts'] - F.lag('unix_ts',1).over(w1)),F.lit(9999.0)))
df_1 = df_1.withColumn('ts_count',F.when(F.col('ts_diff')/60 >= 60,1).otherwise(0))\
           .withColumn("session_num",F.sum('ts_count').over(w1))

df_1 = df_1.withColumn('session_id',F.concat('session_num','anonymous_id'))

#DEBUG: check with a query
#df_1.select("anonymous_id","unix_ts","click_time","ts_diff","ts_count","session_num","session_id").show(truncate=False)

#Debugging - counting uniques 
#df_1.select('session_id').distinct().count()
#df_1.select('filename','session_id').distinct().groupBy('filename').count().show()

#transforming into json
etapa1=df_1.select('file','session_id').distinct().groupBy('file').count()
#etapa1.toJSON().collect()

#Build the data in the way we want to be extracted
etapa1.createOrReplaceTempView("etapa")
etapa1_sql = spark.sql("SELECT CONCAT('\"',file, '\":',count,'\,') x FROM etapa")

#importing SQLContext
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)

#creating with 2 columns and dropping afterwards since it was not working with a sigle column
etapa_header = sqlContext.createDataFrame([("{", "}")],('a', 'b'))
etapa_header=etapa_header.drop("b")

etapa_footer = sqlContext.createDataFrame([("{", "}")],('a', 'b'))
etapa_footer=etapa_footer.drop("a")

#adding header and footer in the dataframe final
etapa1_sql_h=etapa_header.union(etapa1_sql)
etapa1_sql_f=etapa1_sql_h.union(etapa_footer)


#writing to file
etapa1_sql_f.coalesce(1).write.format("text").option("header", "false").mode("append").save("etapa1")

###################################################################

#option 1 to write in json directory I read that this might have problems in Windows
#etapa1.coalesce(1).write.format('json').save('etapa1-dir')
#etapa1_sql.coalesce(1).write.format('json').option("header", "false").save('etapa1-dir')
#etapa1_sql.coalesce(1).write.format('json').option("header", "false").save('etapa1-dir')


#starting etapa2
from pyspark.sql.functions import to_json, spark_partition_id, collect_list, col, struct
df_browser=df_1.select('browser_family','session_id').distinct().groupBy('browser_family').count()
df_os=df_1.select('os_family','session_id').distinct().groupBy('os_family').count()
df_device=df_1.select('device_family','session_id').distinct().groupBy('device_family').count()

etapa2_p1=df_browser.select(to_json(struct(*df_browser.columns)).alias("json"))\
    .agg(collect_list("json").alias("json_list"))\
    .select(col("json_list").cast("string"))

etapa2_p2=df_os.select(to_json(struct(*df_os.columns)).alias("json"))\
    .agg(collect_list("json").alias("json_list"))\
    .select(col("json_list").cast("string"))


etapa2_p3=df_device.select(to_json(struct(*df_device.columns)).alias("json"))\
    .agg(collect_list("json").alias("json_list"))\
    .select(col("json_list").cast("string"))

#etapa2_p1 df will be the consolidated one
etapa2_p1=etapa2_p1.union(etapa2_p2)
etapa2_p1=etapa2_p1.union(etapa2_p3)

#debug
#etapa2_p1.show(truncate=False)

#trying to write json with append
etapa2_p1.coalesce(1).write.format('json').option("header","false").save('etapa2').mode('append')

