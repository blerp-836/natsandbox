# Databricks notebook source
import os
import datetime
import argparse
import shutil
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from utilities.FileLogging import FileLogging
from adls.classes.CustomSchema import CustomSchema
from adls.classes.ConfigLoader import ConfigLoader
from adls.classes.APILandingLoad import APILandingLoad
from adls.classes.EventHubSendCapture import EventHubSendCapture
from adls.classes.EventHubExtract import EventHubExtract
from adls.classes.jsonExtract import jsonExtract
from adls.classes.deltaLoad import deltaLoad
from adls.classes.deltaTransform import deltaTransform
from adls.classes.deltaExtract import deltaExtract
import json

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table ytb_analytics_stg.stg_video_raw;

# COMMAND ----------

blob_account_name='deployblob123'
container_name='deploy'

# COMMAND ----------

dbutils.widgets.text('job','job name')
dbutils.widgets.text('action','action')
dbutils.widgets.text ('tbl','table name')
dbutils.widgets.text('mode','mode')

# COMMAND ----------

job_name=dbutils.widgets.get('job')
tbl=dbutils.widgets.get('tbl')
action=dbutils.widgets.get('action')
mode=dbutils.widgets.get('mode')

# COMMAND ----------

#configure directory for configs
if mode=='dbfs':
    dir=f'/dbfs/mnt/{blob_account_name}/{container_name}'
elif mode=='local':
    dir=os.path.abspath(os.getcwd())

# COMMAND ----------

print(dir)

# COMMAND ----------

#configure directory for logs
path=f'/tmp/{job_name}.log'

# COMMAND ----------

timedlogger=FileLogging(job_name,'INFO',path)
timedlogger.create_timed_rotating_log()

# COMMAND ----------

timedlogger.logger.info('=================={0}-{1}-{2}================='.format(job_name,tbl,action))
#create time rotating logs
timedlogger.logger.info('spark session, dbutils initiated')

# COMMAND ----------

configLoader=ConfigLoader(dir,job_name)
configLoader.read_default_config()
configLoader.read_job_config()

# COMMAND ----------

if action=='landing_load':
    apiLandingLoad=APILandingLoad(dbutils,spark,configLoader,timedlogger,mode)
    apiLandingLoad.landing_load()

# COMMAND ----------

if action=='eventhubsend':
    import time
    eventhubsend=EventHubSendCapture(dbutils,configLoader,timedlogger)
    for i in range(3):
        print('sending event')
        eventhubsend.run_until_complete()
        print('event sent. sleeping')
        time.sleep(65)
    
    print('loop completed sleeping')
    time.sleep(120)
    print('run completed')

# COMMAND ----------

if action=='move_event_hub_to_landing':
    from adls.classes.EventHubLoad import EventHubLoad
    eventhubload=EventHubLoad(dbutils,spark,configLoader,timedlogger,mode)
    eventhubload.move_eventhubcap_to_landing()

    

    

# COMMAND ----------

if action=='eventhubstreaming':
    import time
    eventhubsend=EventHubSendCapture(dbutils,configLoader,timedlogger)
    while True:
        print('sending event')
        eventhubsend.run_until_complete()
        print('event sent. sleeping')
        time.sleep(15)

# COMMAND ----------

if action=='staging_load_i18nRegions':
    configLoader.read_table_config(tbl)
    read_schema=CustomSchema.lnd_ytb_i18nRegions_schema()
    df=jsonExtract(dbutils,spark,configLoader,timedlogger).create_df_from_landing(read_schema)
    delta_transform=deltaTransform(configLoader,timedlogger,spark,dbutils,read_schema,df)
    delta_load=deltaLoad(configLoader,timedlogger,spark,dbutils,df_source=delta_transform.df_source,df_dest=delta_transform.df_dest)
    res=delta_load.append_to_dest()
    assert res==0

# COMMAND ----------

if action=='int_load_i18nRegions':
    configLoader.read_table_config(tbl)
    print(configLoader.tbl_config)
    stg_df=jsonExtract(dbutils,spark,configLoader,timedlogger).create_dim_df_from_stg()
    delta_transform=deltaTransform(configLoader,timedlogger,spark,dbutils,read_schema=None,df=stg_df)
    delta_load=deltaLoad(configLoader,timedlogger,spark,dbutils,df_source=delta_transform.df_source,df_dest=delta_transform.df_dest)
    res=delta_load.append_to_dest()


# COMMAND ----------

if action=='delete_table':
    configLoader.read_table_config(tbl)
    delta_transform=deltaTransform(configLoader,timedlogger,spark,dbutils)
    #delta_transform.read_mapping_csv(tbl)
    #delta_transform.rename_source_to_dest()
    #delta_transform.add_metadata_cols()
    #delta_transform.get_source_pk()
    #int_df=delta_transform.df_source
    #pst_df=delta_transform.df_dest
    stg_dest_delta_table=delta_transform.dest_delta_table
    print(configLoader.tbl_config)
    delta_load=deltaLoad(configLoader,timedlogger,spark,dbutils,df_source=None,df_dest=None,dest_delta_table=stg_dest_delta_table)
    delta_load.delete_dest()

# COMMAND ----------

if action=='pst_load_i18nRegions':
    configLoader.read_table_config(tbl)
    delta_transform=deltaTransform(configLoader,timedlogger,spark,dbutils)
    delta_transform.read_mapping_csv(tbl)
    delta_transform.rename_source_to_dest()
    delta_transform.add_metadata_cols()
    delta_transform.get_source_pk()
    int_df=delta_transform.df_source
    pst_df=delta_transform.df_dest
    pst_dest_delta_table=delta_transform.dest_delta_table
    delta_load=deltaLoad(configLoader,timedlogger,spark,dbutils,df_source=int_df,df_dest=pst_df,dest_delta_table=pst_dest_delta_table)
    res=delta_load.scd1_load()
    assert res==0

# COMMAND ----------

if action=='staging_load_videoCat':
    configLoader.read_table_config(tbl)
    print(configLoader.tbl_config)
    read_schema=CustomSchema.lnd_ytb_videoCat_schema()
    df=jsonExtract(dbutils,spark,configLoader,timedlogger).create_df_from_landing(read_schema)
    delta_transform=deltaTransform(configLoader,timedlogger,spark,dbutils,read_schema,df)
    delta_load=deltaLoad(configLoader,timedlogger,spark,dbutils,df_source=delta_transform.df_source,df_dest=delta_transform.df_dest)
    res=delta_load.append_to_dest()
    assert res==0
    

# COMMAND ----------

if action=='int_load_videoCat':
    configLoader.read_table_config(tbl)
    
    stg_df=jsonExtract(dbutils,spark,configLoader,timedlogger).create_dim_df_from_stg()
    #stg_df.show()
    delta_transform=deltaTransform(configLoader,timedlogger,spark,dbutils,read_schema=None,df=stg_df)
    delta_transform.df_source.show()
    delta_load=deltaLoad(configLoader,timedlogger,spark,dbutils,df_source=delta_transform.df_source,df_dest=delta_transform.df_dest)
    res=delta_load.append_to_dest()
    assert res==0
    

# COMMAND ----------

from pyspark.sql.functions import col, max

# COMMAND ----------

if action=='pst_load_videoCat':
    configLoader.read_table_config(tbl)
    delta_transform=deltaTransform(configLoader,timedlogger,spark,dbutils)
    delta_transform.read_mapping_csv(tbl)
    delta_transform.rename_source_to_dest()
    delta_transform.add_metadata_cols()
    delta_transform.get_source_pk()
    int_df=delta_transform.df_source
    pst_df=delta_transform.df_dest
    pst_dest_delta_table=delta_transform.dest_delta_table
    delta_load=deltaLoad(configLoader,timedlogger,spark,dbutils,df_source=int_df,df_dest=pst_df,dest_delta_table=pst_dest_delta_table)
    res=delta_load.scd1_load()
    assert res==0

# COMMAND ----------

if action=='staging_load_video_raw':
    configLoader.read_table_config(tbl)
    print(configLoader.tbl_config)
    eventhubextract=EventHubExtract(dbutils,spark,configLoader,timedlogger,mode)
    lnd_df=eventhubextract.read_df_from_landing(CustomSchema.lnd_ytb_video_schema())
    stg_df_raw=eventhubextract.create_stg_raw_df(CustomSchema.lnd_ytb_video_schema(),lnd_df)
    delta_transform=deltaTransform(configLoader,timedlogger,spark,dbutils,CustomSchema.lnd_ytb_video_schema(),stg_df_raw)
    delta_load=deltaLoad(configLoader,timedlogger,spark,dbutils,df_source=delta_transform.df_source,df_dest=delta_transform.df_dest)
    res=delta_load.append_to_dest()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT distinct(*) FROM ytb_analytics_int.int_i18n_regions where load_date_time in (select max(load_date_time) from ytb_analytics_int.int_i18n_regions)

# COMMAND ----------

if action=='staging_load_video_processed':
    configLoader.read_table_config(tbl)
    print(configLoader.tbl_config)
    eventhubextract=EventHubExtract(dbutils,spark,configLoader,timedlogger,mode)
    stg_df_processed=eventhubextract.create_stg_processed_df()
    delta_transform=deltaTransform(configLoader,timedlogger,spark,dbutils,read_schema=None,df=stg_df_processed)
    delta_transform.df_source.show()
    delta_load=deltaLoad(configLoader,timedlogger,spark,dbutils,df_source=delta_transform.df_source,df_dest=delta_transform.df_dest)
    res=delta_load.append_to_dest()

# COMMAND ----------

if action=='int_load_video':
    configLoader.read_table_config(tbl)
    print(configLoader.tbl_config)
    eventhubextract=EventHubExtract(dbutils,spark,configLoader,timedlogger,mode)
    int_df=eventhubextract.create_int_df_from_stg_processed()
    delta_transform=deltaTransform(configLoader,timedlogger,spark,dbutils,read_schema=None,df=int_df)
    delta_transform.df_source.show()
    delta_load=deltaLoad(configLoader,timedlogger,spark,dbutils,df_source=delta_transform.df_source,df_dest=delta_transform.df_dest)
    res=delta_load.append_to_dest()

# COMMAND ----------

if action=='pst_load_video':
    configLoader.read_table_config(tbl)
    delta_transform=deltaTransform(configLoader,timedlogger,spark,dbutils)
    delta_transform.read_mapping_csv(tbl)
    delta_transform.join_to_fk_table()
    delta_transform.rename_source_to_dest()
    delta_transform.add_metadata_cols()
    #delta_transform.get_source_pk()
    delta_transform.select_dest_cols()
    int_df=delta_transform.df_source
    pst_df=delta_transform.df_dest
    pst_dest_delta_table=delta_transform.dest_delta_table    
    delta_load=deltaLoad(configLoader,timedlogger,spark,dbutils,df_source=int_df,df_dest=pst_df,dest_delta_table=pst_dest_delta_table)
    #res=delta_load.scd1_load()
    res=delta_load.append_to_dest()
    assert res==0

# COMMAND ----------

if action=='archive':
    src='dbfs:/mnt/natmsdnadlsdatabricks/'+configLoader.config['SETTINGS']['landing']+'/incoming/'
    tgt='dbfs:/mnt/natmsdnadlsdatabricks/'+configLoader.config['SETTINGS']['landing']+'/archive/'
    print(src)
    print(tgt)
    if len(dbutils.fs.ls(src))>0:
        dbutils.fs.mv(src,tgt,True)
        dbutils.fs.mkdirs(src)
        timedlogger.logger.info('archive landing files')
    else:
        timedlogger.logger.info('no landing files. do nothing')
        pass
    

    

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*),modified_ts from ytb_analytics_pst.pst_video group by modified_ts

# COMMAND ----------

import datetime
timestamp=datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S')

# COMMAND ----------

##do this at the end of the run
##move log files to blob
log_files=[f.name for f in dbutils.fs.ls("file:///tmp/") if ".log" in f.name]
log_container='/dbfs/mnt/deployblob123/logs/'

# COMMAND ----------

for log in log_files:
    print("file:///tmp/"+log)
    shutil.copyfile("/tmp/"+log,log_container+log)

# COMMAND ----------

import json
dbutils.notebook.exit(json.dumps({
  "status": "OK",
  "job_name": job_name
}))

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ytb_analytics_stg.stg_video_raw where load_date_time in (select max(load_date_time) from ytb_analytics_stg.stg_video_raw)

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct(load_date_time) from ytb_analytics_stg.stg_video_raw ORDER BY load_date_time desc
