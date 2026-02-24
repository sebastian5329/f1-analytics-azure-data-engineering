# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest circuits.csv file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

v_data_source

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

v_file_date

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DoubleType

# COMMAND ----------

circuits_schema=StructType(fields=[StructField("circuitId",IntegerType(),False),
                StructField("circuitRef",StringType(),True),
                StructField("name",StringType(),True),
                StructField("location",StringType(),True),
                StructField("country",StringType(),True),
                StructField("lat",DoubleType(),True),
                StructField("lng",DoubleType(),True),
                StructField("alt",IntegerType(),True),
                StructField("url",StringType(),True)
                ])

# COMMAND ----------

circuits_df=spark.read.option("header",True).schema(circuits_schema).csv(f"abfss://bronze@databricksf1extadls.dfs.core.windows.net/{v_file_date}/circuits.csv")

# COMMAND ----------

type(circuits_df)

# COMMAND ----------

spark.sql('SHOW CATALOGS').show()

# COMMAND ----------

# MAGIC %sql
# MAGIC show schemas in f1;

# COMMAND ----------

# MAGIC %sql
# MAGIC show tables in f1.bronze;

# COMMAND ----------

circuits_df.schema

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

#selecting the required columns only and renaming
circuits_selected_df=circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),
                    col("lat").alias("latitude"),col("lng").alias("longitude"),col("alt").alias("altitude"))
display(circuits_selected_df)

# COMMAND ----------

#renaming col
circuits_renamed_df=circuits_selected_df.withColumnsRenamed({"circuitId":"circuit_id","circuitRef":"circuit_ref"})
display(circuits_renamed_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

#adding ingestion date to dataframe
circuits_final_df=add_ingestion_date(circuits_renamed_df).withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))
display(circuits_final_df)

# COMMAND ----------

#creating silver table
circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1.silver.circuits")

# COMMAND ----------

df=spark.read.table("f1.silver.circuits")

# COMMAND ----------

display(df)

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

