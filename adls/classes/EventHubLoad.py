import requests, sys, time, os, argparse,datetime,json
from pathlib import Path
import subprocess
from adls.classes.ConfigLoader import ConfigLoader
from utilities import KeyVault,AdlsFs
from adls.classes.CustomSchema import CustomSchema
from adls.classes.JsonLoad import JsonLoad

#get_data
    #get_pages
        #api_request
        #get_video
    #write_to_file

class EventHubLoad():
    def __init__(self,dbutils,spark,configLoader,logger,mode):
        self.mode=mode
        self.configLoader=configLoader
        self.jsonLoad=JsonLoad(dbutils,spark,configLoader,logger)
        self.dbutils=dbutils
        #use dbutils when migrating to databricks
        #self.api_key=self.dbutils.secrets.get(scope = "dbconnect-akv", key = self.configLoader.config['API']['API_KEY'])
        self.keyVault=KeyVault.KeyVault()
        self.api_key=self.keyVault.main(self.configLoader.config['API']['api_key'])
        self.adls_key=self.keyVault.main(self.configLoader.config['SETTINGS']['adls_key'])
        self.spark=spark
        self.request_url=eval(self.configLoader.config['API']['request_url'])
        self.country_code=self.configLoader.config['API']['country_code']
        self.next_page_token=eval(self.configLoader.config['API']['page_token'])
        self.incoming_file=configLoader.config['SETTINGS']['landing_file']
        self.adlsFs=AdlsFs.AdlsFs()
        self.logger=logger.logger
        #self.__getlocal_path()

    def __get_eventhubcap_files_list(self):
        adlspaths=self.adlsFs.list_directory_contents('landing',self.configLoader.config['SETTINGS']['event_hub_landing'])
        avropaths=list()
        for path in adlspaths:
            if '.avro' in path:
                avropaths.append(path)
        return avropaths
    
    def move_eventhubcap_to_landing(self):
        avropaths=self.__get_eventhubcap_files_list()
        for path in avropaths:
            landing_file_name=path.replace('/','_')
            new_path=self.configLoader.config['SETTINGS']['landing']+'/incoming/'+landing_file_name
            command=f"az storage fs file move --new-path {new_path} --path {path} --file-system landing --account-name natmsdnadlsdatabricks\
            --account-key {self.adls_key} --auth-mode key"
            try:
                if self.mode=='local':
                    self.logger.info('running subprocess')
                    subprocess.run(command.split(),check=True)
                    self.logger.info('event hub captures files moved to landing folders')
                else:
                    self.logger.info('running dbutils')
                    path=f'/dbfs/mnt/{self.configLoader.config["SETTINGS"]["storage_account_name"]}/landing/'+path
                    new_path=f'/dbfs/mnt/{self.configLoader.config["SETTINGS"]["storage_account_name"]}/'+new_path
                    #print(path)
                    #print(new_path)
                    os.rename(path,new_path)
            except:
                raise IOError('failed to move file to landing folder')

        
    
    def __read_df_from_landing(self,lnd_body_schema):
        # Convert Body to String and then Json applying the schema
        from pyspark.sql.functions import col,from_json
        path=f'dbfs:/mnt/{self.configLoader.config["SETTINGS"]["storage_account_name"]}/{self.configLoader.config["SETTINGS"]["landing"]}/incoming/'
        df=self.spark.read.format('avro').load(path)
        df = df.withColumn("Body", col("Body").cast("string"))
        jsonOptions = {"dateFormat" : "yyyy-MM-dd HH:mm:ss.SSS"}
        df = df.withColumn("Body", from_json(df.Body,lnd_body_schema, jsonOptions))
        return df
    
    def __write_to_stg_raw(self,key1,key2,lnd_body_schema):
        from pyspark.sql.functions import lit,asc
        raw_stg_df=self.__read_df_from_landing(lnd_body_schema).withColumn('load_date_time',lit(datetime.datetime.now())).withColumn('load_date',lit(datetime.date.today()))\
            .withColumn('countryCode',lit(self.country_code))
        
        raw_stg_df=raw_stg_df.orderBy(asc('sequenceNumber'))
        #raw_stg_df.show()
        path=f'dbfs:/mnt/{self.configLoader.config["SETTINGS"]["storage_account_name"]}/{self.configLoader.config["SETTINGS"]["staging"]}/raw/incoming/'
        raw_stg_df.write.mode('append').partitionBy(key1,key2).format('parquet').save(path)
    
    def __read_from_stg_raw(self,read_schema):
        #read raw df from staging raw
        try:
            from pyspark.sql.functions import col,explode_outer,lit,when
            path=f'dbfs:/mnt/{self.configLoader.config["SETTINGS"]["storage_account_name"]}/{self.configLoader.config["SETTINGS"]["staging"]}/raw/incoming/'
            if read_schema!=None:
                df=self.spark.read.schema(read_schema).format('parquet').load(path).filter(col('countryCode')==self.country_code)
            else:
                df=self.spark.read.format('parquet').load(path).filter(col('countryCode')==self.country_code)
            
            stg_max_date=self.jsonLoad.max_load_date_time(df)
            tempdf=df.filter(col('load_date')==stg_max_date).select('Body')
            # Flatten Body
            for c in df.select('Body').schema['Body'].dataType:
                tempdf= tempdf.withColumn(c.name, col("Body." + c.name))
            
            processed_df=tempdf.drop('Body')

        except:
            raise IOError('failed to create staging work df')

        return processed_df
        

    def __write_to_stg_processed(self,key1,key2,read_schema):
        from pyspark.sql.functions import lit
        processed_df=self.__read_from_stg_raw(read_schema).withColumn('load_date_time',lit(datetime.datetime.now())).withColumn('load_date',lit(datetime.date.today()))\
            .withColumn('countryCode',lit(self.country_code))
        newpath=f'dbfs:/mnt/{self.configLoader.config["SETTINGS"]["storage_account_name"]}/{self.configLoader.config["SETTINGS"]["staging"]}/processed/incoming/'
        processed_df.write.mode('append').partitionBy(key1,key2).format('parquet').save(newpath)

    def load_to_stg(self):
        self.move_eventhubcap_to_landing()
        self.__write_to_stg_raw(self.configLoader.config['API']['key1'],self.configLoader.config['API']['key2'],CustomSchema.lnd_ytb_video_schema())
        self.__write_to_stg_processed(self.configLoader.config['API']['key1'],self.configLoader.config['API']['key2'],None)
