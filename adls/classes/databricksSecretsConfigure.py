# Databricks notebook source
# MAGIC %scala
# MAGIC displayHTML(
# MAGIC    "<b>Privileged DBUtils token (expires in 48 hours): </b>" +
# MAGIC    dbutils.notebook.getContext.apiToken.get.split("").mkString("<span/>"))

# COMMAND ----------

secret_token='dkea74f1feb70e90457531cc99c713ae2a24'

# COMMAND ----------
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
dbutils = DBUtils(spark) 
dbutils.secrets.setToken(secret_token)

# COMMAND ----------

print(dbutils.secrets.get("databricks-akv", "adls-key"))
