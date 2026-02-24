# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

lap_times_schema=StructType(fields=[StructField("raceId",IntegerType(),False),
                                     StructField("driverId",IntegerType(),True),
                                     StructField("lap",IntegerType(),True),
                                     StructField("position",IntegerType(),True),
                                     StructField("time",StringType(),True),
                                     StructField("milliseconds",IntegerType(),True)
                                     ])

# COMMAND ----------

lap_times_df=spark.read.option("header",True).schema(lap_times_schema).csv(f"abfss://bronze@databricksf1extadls.dfs.core.windows.net/{v_file_date}/lap_times")
display(lap_times_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

lap_times_final_df=add_ingestion_date(lap_times_df).withColumnsRenamed({"raceId":"race_id","driverId":"driver_id"}).withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))
display(lap_times_final_df)

# COMMAND ----------

merge_condition="tgt.driver_id=src.driver_id AND tgt.race_id=src.race_id AND tgt.lap=src.lap AND tgt.race_id=src.race_id"
merge_delta_data(lap_times_final_df,"f1","silver","lap_times",merge_condition,"race_id")

# COMMAND ----------

lap_times_processed_df=spark.read.table("f1.silver.lap_times")
display(lap_times_processed_df)

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

