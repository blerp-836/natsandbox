# Databricks notebook source
notebook='delta_workflow'
mode='dbfs'

# COMMAND ----------

dbutils.notebook.run(notebook, 0, {'action':'landing_load','job':'ytb_videoCat','mode':'{file_mode}'.format(file_mode=mode),'tbl':'lnd_video_cat'})

# COMMAND ----------

dbutils.notebook.run(notebook, 0, {'action':'staging_load_videoCat','job':'ytb_videoCat','mode':'{file_mode}'.format(file_mode=mode),'tbl':'stg_video_cat'})

# COMMAND ----------

dbutils.notebook.run(notebook, 0, {'action':'int_load_videoCat','job':'ytb_videoCat','mode':'{file_mode}'.format(file_mode=mode),'tbl':'int_video_cat'})

# COMMAND ----------


dbutils.notebook.run(notebook, 0, {'action':'pst_load_videoCat','job':'ytb_videoCat','mode':'{file_mode}'.format(file_mode=mode),'tbl':'pst_video_cat'})


# COMMAND ----------


dbutils.notebook.run(notebook, 0, {'action':'archive','job':'ytb_videoCat','mode':'{file_mode}'.format(file_mode=mode),'tbl':'videoCategories'})

