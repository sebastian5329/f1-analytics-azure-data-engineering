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

pit_stops_schema=StructType(fields=[StructField("raceId",IntegerType(),False),
                                     StructField("driverId",IntegerType(),True),
                                     StructField("stop",StringType(),True),
                                     StructField("lap",IntegerType(),True),
                                     StructField("time",StringType(),True),
                                     StructField("duration",StringType(),True),
                                     StructField("milliseconds",IntegerType(),True)
                                     ])

# COMMAND ----------

pit_stops_df=spark.read.option("header",True).schema(pit_stops_schema).option("multiline",True).json(f"abfss://bronze@databricksf1extadls.dfs.core.windows.net/{v_file_date}/pit_stops.json")
display(pit_stops_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

pit_stops_final_df=add_ingestion_date(pit_stops_df).withColumnsRenamed({"raceId":"race_id","driverId":"driver_id"}).withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))
display(pit_stops_final_df)

# COMMAND ----------

merge_condition="tgt.driver_id=src.driver_id AND tgt.race_id=src.race_id AND tgt.stop=src.stop AND tgt.race_id=src.race_id"
merge_delta_data(pit_stops_final_df,"f1","silver","pit_stops",merge_condition,"race_id")

# COMMAND ----------

pit_stops_processed_df=spark.read.table("f1.silver.pit_stops")
display(pit_stops_processed_df)

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

