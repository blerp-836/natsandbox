# Databricks notebook source
dbutils.notebook.run("notebook_workflow", 0, {'action':'eventhubsend','job':'ytb_video','mode':'dbfs','tbl':'ytb_video'})

# COMMAND ----------

dbutils.notebook.run("notebook_workflow", 0, {'action':'eventhubload','job':'ytb_video','mode':'dbfs','tbl':'ytb_video'})

# COMMAND ----------

dbutils.notebook.run("notebook_workflow", 0, {'action':'int_load_video','job':'ytb_video','mode':'dbfs','tbl':'ytb_video'})
