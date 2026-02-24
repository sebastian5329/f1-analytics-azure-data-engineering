# Databricks notebook source
# MAGIC %md
# MAGIC #Ingest races.csv

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

display(spark.read.option("header",True).csv(f"abfss://bronze@databricksf1extadls.dfs.core.windows.net/{v_file_date}/races.csv"))

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,DateType

# COMMAND ----------

races_schema=StructType(fields=[StructField("raceId",IntegerType(),False),
                              StructField("year",IntegerType(),True),
                              StructField("round",IntegerType(),True),
                              StructField("circuitId",IntegerType(),True),
                              StructField("name",StringType(),True),
                              StructField("date",DateType(),True),
                              StructField("time",StringType(),True),
                              StructField("url",StringType(),True)])

# COMMAND ----------

races_df=spark.read.option("header",True).schema(races_schema).csv(f"abfss://bronze@databricksf1extadls.dfs.core.windows.net/{v_file_date}/races.csv")

# COMMAND ----------

display(races_df)

# COMMAND ----------

#renaming cols
races_renamed_df=races_df.withColumnsRenamed({"raceId":"race_id","year":"race_year","circuitId":"circuit_id"})
display(races_renamed_df)

# COMMAND ----------

# DBTITLE 1,Cell 12
#combining date and time cols and adding ingestion date col
from pyspark.sql.functions import concat,lit,to_timestamp,when,col
races_with_timestamp_df = add_ingestion_date(races_renamed_df)\
.withColumn("race_timestamp",to_timestamp(concat(col("date"),lit(" "),when(col("time")=="\\N",lit("00:00:00")).otherwise(col("time"))), 'yyyy-MM-dd HH:mm:ss')).withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))
display(races_with_timestamp_df)

# COMMAND ----------

#selecting only required cols
races_selected_df=races_with_timestamp_df.select(col("race_id"),col("race_year"),col("round"),col("circuit_id"),col("name"),col("ingestion_date"),col("race_timestamp"),col("data_source"),col("file_date"))
display(races_selected_df)

# COMMAND ----------

#creating table in silver
races_selected_df.write.mode("overwrite").format("delta").saveAsTable("f1.silver.races")

# COMMAND ----------

races_processed_df=spark.read.table("f1.silver.races")
display(races_processed_df)

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

