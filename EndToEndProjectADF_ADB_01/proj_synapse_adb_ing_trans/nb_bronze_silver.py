# Databricks notebook source
dbutils.fs.ls("/mnt/silver/SalesLT")

# COMMAND ----------

# MAGIC %md
# MAGIC #Assigning bronze path and show table content

# COMMAND ----------


#spark.catalog.clearCache()
#This cell is only for to test input_path and check for address data only
input_path = '/mnt/bronze/SalesLT/Address/Address.parquet'
df = spark.read.parquet(input_path)
df.show()


# COMMAND ----------

# MAGIC %md
# MAGIC #Read and write/load Data in dataframe from ADLS
# MAGIC #### In memory - databricks stores data in memory cluster for fast access and the size all depends on the
# MAGIC ####size you assigned for cluster. DBFS however, is different storage (its just like harddisk space in VM)

# COMMAND ----------

df = spark.read.format('parquet').load(input_path)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Doing tranformation for all the tables in bronze

# COMMAND ----------

table_name = []

for i in dbutils.fs.ls('mnt/bronze/SalesLT/'):
    table_name.append(i.name.split('/')[0])

# COMMAND ----------

table_name

# COMMAND ----------

from pyspark.sql.functions import from_utc_timestamp,date_format
from pyspark.sql.types import TimestampType

for i in table_name:
    path ='/mnt/bronze/SalesLT/' + i + '/' + i + '.parquet'
    df = spark.read.format('parquet').load(path)
    column = df.columns

    for col in column:
        if "Date" in col or "date" in col:
            df= df.withColumn(col,date_format(from_utc_timestamp(df[col].cast(TimestampType()),"UTC"),"yyyy-MM-dd"))
    output_path ='/mnt/silver/SalesLT/' + i +'/'
    df.write.format('delta').mode("overwrite").save(output_path)
    

# COMMAND ----------

# MAGIC %md
# MAGIC #Verify the data in Destination /'mnt/SalesLT/Silver

# COMMAND ----------

input_path = '/mnt/bronze/SalesLT/Address/Address.parquet'
df = spark.read.parquet(input_path)
df.show()

#Alternate method short
# spark.read.parquet('/mnt/bronze/SalesLT/Address/Address.parquet').show()

##If you want to display in tabular format
#display(spark.read.parquet('/mnt/bronze/SalesLT/Address/Address.parquet'))



# COMMAND ----------


