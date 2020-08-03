from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
sc = SparkContext('local')
spark = SparkSession(sc)
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import input_file_name
from pyspark.sql.functions import from_unixtime

#First dataframe loads the "data" directory
df = spark.read.json("data")

#df2 adds a column filename on it.
df2 = df.withColumn("filename", input_file_name())
#df2.show()

#df3 adds a column t3 with the timestamp converted to date
df3 = df2.withColumn('t3',from_unixtime((df2['device_sent_timestamp'])/1000))

#test outputs
#df3.groupBy('filename').count().show()
#df3.groupBy('anonymous_id').count().show()
#df3.printSchema()

#transforming into json
etapa1=df3.groupBy('filename').count()
etapa1.toJSON().collect()

#option 1 to write in json directory I read that this might have problems in Windows
etapa1.coalesce(1).write.format('json').save('etapa1-dir')

#starting etapa2
df_browser=df3.groupBy('browser_family').count()
df_os=df3.groupBy('os_family').count()
df_device=df3.groupBy('device_family').count()

#trying to write json with append
df_browser.coalesce(1).write.format('json').save('etapa2-browser')
df_os.coalesce(1).write.format('json').save('etapa2-browser').mode('append')
df_device.coalesce(1).write.format('json').save('etapa2-browser').mode('append')

