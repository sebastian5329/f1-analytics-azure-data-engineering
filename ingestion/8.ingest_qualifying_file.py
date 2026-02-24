# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

# COMMAND ----------

qualifying_schema=StructType(fields=[StructField("qualifyId",IntegerType(),False),
                                     StructField("raceId",IntegerType(),True),
                                     StructField("driverId",IntegerType(),True),
                                     StructField("constructorId",IntegerType(),True),
                                     StructField("number",IntegerType(),True),
                                     StructField("position",IntegerType(),True),
                                     StructField("q1",StringType(),True),
                                     StructField("q2",StringType(),True),
                                     StructField("q3",StringType(),True)
                                     ])

# COMMAND ----------

qualifying_df=spark.read.option("header",True).schema(qualifying_schema).option("multiline",True).json(f"abfss://bronze@databricksf1extadls.dfs.core.windows.net/{v_file_date}/qualifying")
display(qualifying_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

qualifying_final_df=add_ingestion_date(qualifying_df).withColumnsRenamed({"qualifyId":"qualify_id","raceId":"race_id","driverId":"driver_id","constructorId":"constructor_id"}).withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))
display(qualifying_final_df)

# COMMAND ----------

merge_condition="tgt.qualify_id=src.qualify_id AND tgt.race_id=src.race_id"
merge_delta_data(qualifying_final_df,"f1","silver","qualifying",merge_condition,"race_id")

# COMMAND ----------

qualifying_processed_df=spark.read.table("f1.silver.qualifying")
display(qualifying_processed_df)

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

