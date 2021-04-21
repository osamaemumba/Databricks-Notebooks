# Databricks notebook source
spark.conf.set(
  "fs.azure.account.key.msft2datalake.dfs.core.windows.net",
  "FtOtK3zt46LkpTCAX9D0WZrnWsaibl7TxBhNChtD/WJ6BB3qc17lkQufMP0nV5PSmHyrJ+ZgoBpS5D559j1Bvw=="
)

# COMMAND ----------

#set the data lake file location:
file_location = "abfss://data-capture@msft2datalake.dfs.core.windows.net/raw/raw.csv"
 
#read in the data to dataframe df
df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimiter",",").load(file_location)
 
#display the dataframe
display(df)

# COMMAND ----------

#set the data lake file location:
file_location = "abfss://data-capture@msft2datalake.dfs.core.windows.net/raw/raw.csv"
 
#read in the data to dataframe df
df = spark.read.format("csv").option("inferSchema", "true").option("header", "true").option("delimiter",",").load(file_location)
 
#add new column
#for col in df.columns:
#    df = df.withColumnRenamed(col, col.upper())
df = df.withColumnRenamed('created_at', 'timestamp')
    
#display the dataframe
display(df)

# COMMAND ----------

#declare data lake path where we want to write the data
target_file = "abfss://data-capture@msft2datalake.dfs.core.windows.net/curated_tmp"
 
#write as csv data
df.repartition(1).write.mode('overwrite').options(header='True', delimiter=',').csv(target_file)

# COMMAND ----------

data_location = "abfss://data-capture@msft2datalake.dfs.core.windows.net/curated_tmp/"
save_location = "abfss://data-capture@msft2datalake.dfs.core.windows.net/curated/curated/"
files = dbutils.fs.ls(data_location)
csv_file = [x.path for x in files if x.path.endswith(".csv")][0]
dbutils.fs.mv(csv_file, save_location.rstrip('/') + ".csv")
dbutils.fs.rm(data_location, recurse = True)
