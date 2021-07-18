# Databricks notebook source
dbutils.notebook.run("notebook_workflow", 0, {'action':'landing_load','job':'ytb_videoCat','mode':'dbfs','tbl':'ytb_videoCat'})

# COMMAND ----------

dbutils.notebook.run("notebook_workflow", 0, {'action':'staging_load_videoCat','job':'ytb_videoCat','mode':'dbfs','tbl':'ytb_videoCat'})

# COMMAND ----------

dbutils.notebook.run("notebook_workflow", 0, {'action':'int_load_videoCat','job':'ytb_videoCat','mode':'dbfs','tbl':'ytb_videoCat'})

# COMMAND ----------




