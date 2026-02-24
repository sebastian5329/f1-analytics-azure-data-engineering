# Databricks notebook source
# MAGIC %md
# MAGIC # Ingest drivers file

# COMMAND ----------

dbutils.widgets.text("p_data_source","")
v_data_source=dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date=dbutils.widgets.get("p_file_date")

# COMMAND ----------

display(spark.read.json(f"abfss://bronze@databricksf1extadls.dfs.core.windows.net/{v_file_date}/drivers.json"))

# COMMAND ----------

# MAGIC %run ../includes/common_functions

# COMMAND ----------

    from pyspark.sql.types import StructType,StructField,IntegerType,StringType,DateType

# COMMAND ----------

name_schema=StructType(fields=[StructField("forename",StringType(),True),
                               StructField("surname",StringType(),True)
                               ])

# COMMAND ----------

drivers_schema=StructType(fields=[StructField("code",StringType(),True),
                                  StructField("dob",DateType(),True),
                                  StructField("driverId",IntegerType(),True),
                                  StructField("driverRef",StringType(),True),
                                  StructField("name",name_schema),
                                  StructField("nationality",StringType(),True),
                                  StructField("number",IntegerType(),True),
                                  StructField("url",StringType(),True)
                                  ])

# COMMAND ----------

drivers_df=spark.read.option("header",True).schema(drivers_schema).json(f"abfss://bronze@databricksf1extadls.dfs.core.windows.net/{v_file_date}/drivers.json")
display(drivers_df)

# COMMAND ----------

#renaming cols
drivers_renamed_df=drivers_df.withColumnsRenamed({"driverId":"driver_id","driverRef":"driver_ref"})
display(drivers_renamed_df)

# COMMAND ----------

from pyspark.sql.functions import concat,col,lit
drivers_concat_df=drivers_renamed_df.withColumn("name",concat(col("name.forename"),lit(" "),col("name.surname"))).withColumn("data_source",lit(v_data_source)).withColumn("file_date",lit(v_file_date))
display(drivers_concat_df)

# COMMAND ----------

#adding ingestion date col and removing url col
drivers_final_df=add_ingestion_date(drivers_concat_df).drop("url")
display(drivers_final_df)

# COMMAND ----------

#creating silver table
drivers_final_df.write.mode("overwrite").format("delta").saveAsTable("f1.silver.drivers")

# COMMAND ----------

drivers_processed_df=spark.read.table("f1.silver.drivers")
display(drivers_processed_df)

# COMMAND ----------

dbutils.notebook.exit("success")

# COMMAND ----------

