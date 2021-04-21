# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.msft2datalake.dfs.core.windows.net",
  "FtOtK3zt46LkpTCAX9D0WZrnWsaibl7TxBhNChtD/WJ6BB3qc17lkQufMP0nV5PSmHyrJ+ZgoBpS5D559j1Bvw=="
)

# COMMAND ----------

#set the data lake file location:
#file_location = 'abfss://data-capture@msft2datalake.dfs.core.windows.net/msft2eventhubs/sample-evenhub/0/*/*/*/*/*/*.avro'
 
#read in the data to dataframe df
#df = spark.read.format("avro").load(file_location, header = 'true')


#display the dataframe
#display(df)

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession \
    .builder \
    .appName("spark-avro-json-sample") \
    .config('spark.hadoop.avro.mapred.ignore.inputs.without.extension', 'false') \
    .getOrCreate()

dbutils.widgets.text("files_to_be_read", "","")
y = dbutils.widgets.get("files_to_be_read")
print (y)

in_path = f'abfss://data-capture@msft2datalake.dfs.core.windows.net/msft2eventhubs/sample-evenhub/0/{y}/*/*/*.avro'
print(in_path)
#storage->avro
avroDf = spark.read.format("com.databricks.spark.avro").load(in_path)
#avro->json
jsonRdd = avroDf.select(avroDf.Body.cast("string")).rdd.map(lambda x: x[0])
data = spark.read.json(jsonRdd) # in real world it's better to specify a schema for the JSON
#data.show()
print(data.count())

#get distinct records/rows only
#distinct_data = data.distinct()

#alternate way to get distinct records/rows - in this way we can specify a column(s) based distinct records
distinct_data = data.dropDuplicates(['device_id', 'timestamp'])

print(distinct_data.count())

# COMMAND ----------

#set the data lake file location:
target_file = "abfss://data-capture@msft2datalake.dfs.core.windows.net/raw_temp"

#write as csv data
distinct_data.repartition(1).write.mode('overwrite').options(header='True', delimiter=',').csv(target_file)

# COMMAND ----------

data_location = "abfss://data-capture@msft2datalake.dfs.core.windows.net/raw_temp/"
save_location = "abfss://data-capture@msft2datalake.dfs.core.windows.net/raw/raw/"
files = dbutils.fs.ls(data_location)
csv_file = [x.path for x in files if x.path.endswith(".csv")][0]
dbutils.fs.mv(csv_file, save_location.rstrip('/') + ".csv")
dbutils.fs.rm(data_location, recurse = True)
