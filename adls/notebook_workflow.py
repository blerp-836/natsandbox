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
from adls.classes.JsonLoad import JsonLoad
from adls.classes.EventHubSendCapture import EventHubSendCapture
from adls.classes.EventHubLoad import EventHubLoad
import json


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
dir=f'/dbfs/mnt/{blob_account_name}/{container_name}'

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

if action=='staging_load_i18nRegions':
  jsonLoad=JsonLoad(dbutils,spark,configLoader,timedlogger)
  jsonLoad.staging_load(CustomSchema.lnd_ytb_i18nRegions_schema())

# COMMAND ----------

if action=='int_load_i18nRegions':
  jsonLoad=JsonLoad(dbutils,spark,configLoader,timedlogger)
  jsonLoad.int_load(CustomSchema.stg_ytb_i18nRegions_schema(),'i18nRegions')

# COMMAND ----------

if action=='staging_load_videoCat':
  jsonLoad=JsonLoad(dbutils,spark,configLoader,timedlogger)
  jsonLoad.staging_load(CustomSchema.lnd_ytb_videoCat_schema())

# COMMAND ----------

if action=='int_load_videoCat':
  jsonLoad=JsonLoad(dbutils,spark,configLoader,timedlogger)
  jsonLoad.int_load(CustomSchema.stg_ytb_videoCat_schema(),'videoCat')

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

if action=='eventhubload':
  eventhubload=EventHubLoad(dbutils,spark,configLoader,timedlogger,mode)
  eventhubload.load_to_stg()

# COMMAND ----------

if action=='int_load_video':
    jsonLoad=JsonLoad(dbutils,spark,configLoader,timedlogger)
    jsonLoad.int_load(CustomSchema.stg_ytb_video_schema(),'video')

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

import datetime
timestamp=datetime.datetime.now().strftime('%Y-%m-%d_%H%M%S')

# COMMAND ----------

##do this at the end of the run
##move log files to blob
log_files=[f.name for f in dbutils.fs.ls("file:///tmp/") if ".log" in f.name]
log_container='/dbfs/mnt/deployblob123/logs/'

# COMMAND ----------

log_files

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

# for testing only
#import configparser
#config=configparser.ConfigParser()
#config.read(dir+job_name+'/config/env.ini')
#config.sections()
