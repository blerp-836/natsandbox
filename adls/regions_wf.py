# Databricks notebook source
dbutils.notebook.run("notebook_workflow", 0, {'action':'landing_load','job':'ytb_i18nRegions','mode':'dbfs','tbl':'ytb_i18nRegions'})

# COMMAND ----------

dbutils.notebook.run("notebook_workflow", 0, {'action':'staging_load_i18nRegions','job':'ytb_i18nRegions','mode':'dbfs','tbl':'ytb_i18nRegions'})

# COMMAND ----------

dbutils.notebook.run("notebook_workflow", 0, {'action':'int_load_i18nRegions','job':'ytb_i18nRegions','mode':'dbfs','tbl':'ytb_i18nRegions'})
