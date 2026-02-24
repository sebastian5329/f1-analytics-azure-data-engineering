# Databricks notebook source
dbutils.widgets.text("p_file_date","2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

spark.sql(f"""create table if not exists f1.gold.calculated_race_results
          (
              race_year int,
              team_name string,
              driver_id int,
              driver_name string,
              race_id int,
              position int,
              points int,
              calculated_points int,
              created_date timestamp,
              updated_date timestamp
          )
""")

# COMMAND ----------

spark.sql(f"""
          create or replace temp view race_results_updated
          as
          select races.race_year,
                constructors.name as team_name,
                drivers.driver_id,
                drivers.name as driver_name,
                races.race_id,
                results.position,
                results.points,
                11 - results.position as calculated_points
          from f1.silver.results
          join f1.silver.drivers on results.driver_id=drivers.driver_id
          join f1.silver.constructors on results.constructor_id=constructors.constructor_id
          join f1.silver.races on results.race_id=races.race_id
          where results.position<= 10
          and results.file_date = '{v_file_date}'
""")

# COMMAND ----------

spark.sql(f"""
            merge into f1.gold.calculated_race_results tgt
            using race_results_updated upd
            on (tgt.driver_id = upd.driver_id and tgt.race_id = upd.race_id)
            when matched then
                update set tgt.position = upd.position,
                        tgt.points = upd.points,
                        tgt.calculated_points = upd.calculated_points,
                        tgt.updated_date = current_timestamp
            when not matched then
                insert(race_year, team_name, driver_id, driver_name, race_id, position, points,
                calculated_points, created_date)
                values(race_year, team_name, driver_id, driver_name, race_id, position, points,
                calculated_points, current_timestamp)
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1.gold.calculated_race_results;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1)from race_results_updated;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(1)from f1.gold.calculated_race_results;