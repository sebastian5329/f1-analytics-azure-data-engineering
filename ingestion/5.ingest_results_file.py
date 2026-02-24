# Databricks notebook source
dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

display(spark.read.json(f"abfss://bronze@databricksf1extadls.dfs.core.windows.net/{v_file_date}/results.json"))

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,FloatType
results_schema=StructType(fields=[StructField("constructorId",IntegerType(),True),
                                  StructField("driverId",IntegerType(),True),
                                  StructField("fastestLap",IntegerType(),True),
                                  StructField("fastestLapSpeed",FloatType(),True),
                                  StructField("fastestLapTime",StringType(),True),
                                  StructField("grid",IntegerType(),True),
                                  StructField("laps",IntegerType(),True),
                                  StructField("milliseconds",IntegerType(),True),
                                  StructField("number",IntegerType(),True),
                                  StructField("points",FloatType(),True),
                                  StructField("position",IntegerType(),True),
                                  StructField("positionOrder",IntegerType(),True),
                                  StructField("positionText",StringType(),True),
                                  StructField("raceId",IntegerType(),True),
                                  StructField("rank",IntegerType(),True),
                                  StructField("resultId",IntegerType(),False),
                                  StructField("statusId",StringType(),True),
                                  StructField("time",StringType(),True)
                                  ])

# COMMAND ----------

results_df=spark.read.option("header",True).schema(results_schema).json(f"abfss://bronze@databricksf1extadls.dfs.core.windows.net/{v_file_date}/results.json")
display(results_df)

# COMMAND ----------

len(results_df.columns)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

#renaming cols and dropping statusId
results_renamed_df=results_df.withColumnsRenamed({"constructorId":"constructor_id","resultId":"result_id",
                                                  "raceId":"race_id","driverId":"driver_id","positionText":"position_text",
                                                  "positionOrder":"position_order","fastestLap":"fastest_lap","fastestLapTime":"fastest_lap_time","fastestLapSpeed":"fastest_lap_speed"}).drop(col("statusId"))
display(results_renamed_df)

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

#adding Ingestion date
results_final_df=add_ingestion_date(results_renamed_df).withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))
display(results_final_df)

# COMMAND ----------

#de-duping df
results_deduped_df=results_final_df.dropDuplicates(['race_id','driver_id'])

# COMMAND ----------

merge_condition="tgt.result_id=src.result_id AND tgt.race_id=src.race_id"
merge_delta_data(results_deduped_df,"f1","silver","results",merge_condition,"race_id")

# COMMAND ----------

results_processed_df=spark.read.table("f1.silver.results")
display(results_processed_df)

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

