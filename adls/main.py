
from pathlib import Path
TOP_DIR = Path(__file__).resolve().parent.parent
import sys
sys.path.append(str(TOP_DIR))
print(sys.path)
import os
import datetime
import argparse

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



if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-j", "--job_name", required=True ,help="Job unique id for monitoring purposes, should be provided by support/devops team")
    parser.add_argument("-a","--action", required=True, help="Action to be executed, possible values are [create-table | extract | load ]")
    parser.add_argument("-t","--tbl", required=False, help="Table entity name")
    parser.add_argument('-m','--mode',required=True, help="databricks priviledged dbutils token. need to be refreshed every 48 hours")

    args = parser.parse_args()
    job_name = args.job_name
    tbl = args.tbl
    action = args.action
    mode=args.mode    

    start = datetime.datetime.now()

    if mode=='local':
        dir = os.path.dirname(os.path.abspath(__file__))
    else:
        #put the dbfs path
        pass
    
    if not os.path.exists(dir+"/logs/"):
        os.makedirs(dir+"/logs/")
    
    path=dir+"/logs/{0}.log".format(job_name)
    
    timedlogger=FileLogging(job_name,'INFO',path)
    timedlogger.create_timed_rotating_log()
    
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark) 

    #set dbutils priviledged token
    
    timedlogger.logger.info('=================={0}-{1}-{2}================='.format(job_name,tbl,action))
    #create time rotating logs
    timedlogger.logger.info('spark session, dbutils initiated')

    #create config object
    configLoader=ConfigLoader(dir,job_name)
    configLoader.read_default_config()
    configLoader.read_job_config()
       

    if action=='landing_load':
        apiLandingLoad=APILandingLoad(dbutils,spark,configLoader,timedlogger,mode)
        apiLandingLoad.landing_load()
        #spark.read.schema(CustomSchema.landing_schema()).option('multiline','true').json('dbfs:/temp/ytb.json').limit(2).show()


    if action=='staging_load_videoCat':
        jsonLoad=JsonLoad(dbutils,spark,configLoader,timedlogger)
        jsonLoad.staging_load(CustomSchema.lnd_ytb_videoCat_schema())
    
    if action=='staging_load_i18nRegions':
        jsonLoad=JsonLoad(dbutils,spark,configLoader,timedlogger)
        jsonLoad.staging_load(CustomSchema.lnd_ytb_i18nRegions_schema())

    if action=='int_load_video':
        jsonLoad=JsonLoad(dbutils,spark,configLoader,timedlogger)
        jsonLoad.int_load(CustomSchema.stg_ytb_video_schema(),'video')
    
    if action=='int_load_videoCat':
        jsonLoad=JsonLoad(dbutils,spark,configLoader,timedlogger)
        jsonLoad.int_load(CustomSchema.stg_ytb_videoCat_schema(),'videoCat')
    
    if action=='int_load_i18nRegions':
        
        jsonLoad=JsonLoad(dbutils,spark,configLoader,timedlogger)
        jsonLoad.int_load(CustomSchema.stg_ytb_i18nRegions_schema(),'i18nRegions')

    if action=='archived_adls':
        apiLandingLoad=APILandingLoad(dbutils,spark,configLoader,timedlogger)
        apiLandingLoad.move_to_archive()
        
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
        
    if action=='eventhubload':
        eventhubload=EventHubLoad(dbutils,spark,configLoader,timedlogger,mode)
        eventhubload.load_to_stg()

    if action=='eventhubstreaming':
        import time
        eventhubsend=EventHubSendCapture(dbutils,configLoader,timedlogger)
        while True:
            print('sending event')
            eventhubsend.run_until_complete()
            print('event sent. sleeping')
            time.sleep(15)

        
            
        





