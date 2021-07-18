# Databricks notebook source
spark.catalog.refreshTable('int_i18nRegions')

# COMMAND ----------

spark.catalog.refreshTable("int_video")

# COMMAND ----------

spark.catalog.refreshTable("int_videocategories")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from int_i18nRegions order by load_date_time desc limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from int_videocategories order by load_date_time desc limit 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from int_video order by load_date_time desc limit 10;
