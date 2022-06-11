# Databricks notebook source
notebook='delta_workflow'
mode='dbfs'

# COMMAND ----------

dbutils.notebook.run(notebook, 0, {'action':'eventhubsend','job':'ytb_video','mode':'{file_mode}'.format(file_mode=mode),'tbl':'lnd_video'})

# COMMAND ----------

dbutils.notebook.run(notebook, 0, {'action':'move_event_hub_to_landing','job':'ytb_video','mode':'{file_mode}'.format(file_mode=mode),'tbl':'lnd_video'})

# COMMAND ----------

dbutils.notebook.run(notebook, 0, {'action':'staging_load_video_raw','job':'ytb_video','mode':'{file_mode}'.format(file_mode=mode),'tbl':'stg_video_raw'})

# COMMAND ----------

dbutils.notebook.run(notebook, 0, {'action':'staging_load_video_processed','job':'ytb_video','mode':'{file_mode}'.format(file_mode=mode),'tbl':'stg_video_processed'})

# COMMAND ----------

dbutils.notebook.run(notebook, 0, {'action':'int_load_video','job':'ytb_video','mode':'{file_mode}'.format(file_mode=mode),'tbl':'int_video'})

# COMMAND ----------

dbutils.notebook.run(notebook, 0, {'action':'pst_load_video','job':'ytb_video','mode':'{file_mode}'.format(file_mode=mode),'tbl':'pst_video'})

# COMMAND ----------

dbutils.notebook.run(notebook, 0, {'action':'archive','job':'ytb_video','mode':'{file_mode}'.format(file_mode=mode),'tbl':'video'})
