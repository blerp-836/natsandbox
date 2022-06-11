# Databricks notebook source
notebook='delta_workflow'
mode='dbfs'

# COMMAND ----------

dbutils.notebook.run(notebook, 0, {'action':'landing_load','job':'ytb_i18nRegions','mode':'{file_mode}'.format(file_mode=mode),'tbl':'ytb_i18nRegions'})

# COMMAND ----------

dbutils.notebook.run(notebook, 0, {'action':'staging_load_i18nRegions','job':'ytb_i18nRegions','mode':'{file_mode}'.format(file_mode=mode),'tbl':'stg_i18n_regions'})

# COMMAND ----------

dbutils.notebook.run(notebook, 0, {'action':'int_load_i18nRegions','job':'ytb_i18nRegions','mode':'{file_mode}'.format(file_mode=mode),'tbl':'int_i18n_regions'})

# COMMAND ----------

dbutils.notebook.run(notebook, 0, {'action':'pst_load_i18nRegions','job':'ytb_i18nRegions','mode':'{file_mode}'.format(file_mode=mode),'tbl':'pst_i18n_regions'})

# COMMAND ----------

dbutils.notebook.run(notebook, 0, {'action':'archive','job':'ytb_i18nRegions','mode':'{file_mode}'.format(file_mode=mode),'tbl':'i18nRegions'})
