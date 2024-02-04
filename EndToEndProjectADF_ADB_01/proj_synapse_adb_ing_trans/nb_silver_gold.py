# Databricks notebook source
dbutils.fs.ls('/mnt/silver/SalesLT')

# COMMAND ----------

dbutils.fs.ls('/mnt/s')

# COMMAND ----------

input_path = '/mnt/silver/SalesLT/Address/'

# COMMAND ----------

df= spark.read.format('delta').load(input_path)
display(df)


# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace

#Get list of column names

column_names = df.columns
for old_col_name in column_names:
    #convert columnName to column_name format
    new_col_name = "".join(["_" + char if char.isupper() and not old_col_name[i-1].isupper() else char for i, char in enumerate(old_col_name)]).lstrip("_")

    #change the column name using withColumnRenamed and regex_replace
    df= df.withColumnRenamed(old_col_name, new_col_name)

# COMMAND ----------

display(df)

# COMMAND ----------

table_name = []

for i in dbutils.fs.ls('/mnt/silver/SalesLT/'):
    table_name.append(i.name.split('/')[0])

# COMMAND ----------

table_name

# COMMAND ----------

for name in table_name:
    input_path = '/mnt/silver/SalesLT/' + name
    print(input_path)
    df=spark.read.format('delta').load(input_path)
    column_name = df.columns

    for old_col in column_name:
        new_col= "".join(["_" + char if char.isupper() and not old_col[i-1].isupper() else char for i, char in enumerate(old_col)]).lstrip("_")

        df= df.withColumnRenamed(old_col, new_col)

    output_path = 'mnt/gold/SalesLT' +name + '/'
    #display(output_path)
    df.write.format('delta').mode("overwrite").save(output_path)

# COMMAND ----------

    display(input_path)

# COMMAND ----------


