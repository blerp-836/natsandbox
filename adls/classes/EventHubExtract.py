import requests, sys, time, os, argparse,datetime,json
from pathlib import Path
import subprocess
from adls.classes.ConfigLoader import ConfigLoader
from utilities import KeyVault,AdlsFs
from adls.classes.CustomSchema import CustomSchema
from adls.classes.jsonExtract import jsonExtract

#get_data
    #get_pages
        #api_request
        #get_video
    #write_to_file

class EventHubExtract():
    def __init__(self,dbutils,spark,configLoader,logger,mode):
        self.mode=mode
        self.configLoader=configLoader
        self.jsonExtract=jsonExtract(dbutils,spark,configLoader,logger)
        self.dbutils=dbutils
        #use dbutils when migrating to databricks
        self.api_key=self.dbutils.secrets.get(scope = "databricks-akv", key = self.configLoader.config['API']['API_KEY'])
        #self.keyVault=KeyVault.KeyVault()
        #self.api_key=self.keyVault.main(self.configLoader.config['API']['api_key'])
        self.adls_key=self.dbutils.secrets.get(scope = "databricks-akv", key = self.configLoader.config['SETTINGS']['adls_key'])
        #self.adls_key=self.keyVault.main(self.configLoader.config['SETTINGS']['adls_key'])
        self.spark=spark
        self.request_url=eval(self.configLoader.config['API']['request_url'])
        self.country_code=self.configLoader.config['API']['country_code']
        self.next_page_token=eval(self.configLoader.config['API']['page_token'])
        self.incoming_file=configLoader.config['SETTINGS']['landing_file']
        self.adlsFs=AdlsFs.AdlsFs()
        self.logger=logger.logger
        #self.__getlocal_path()

    def max_load_date(self,df):
        max_time_col=df.select(self.configLoader.config['DEST']['DELTA_COL']).groupBy().agg({self.configLoader.config['DEST']['DELTA_COL']:'max'}).collect()[0][0]
        return max_time_col
    
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

        
    
    def read_df_from_landing(self,lnd_body_schema):
        #read from landing
        # Convert Body to String and then Json applying the schema
        from pyspark.sql.functions import col,from_json
        path=f'dbfs:/mnt/{self.configLoader.config["SETTINGS"]["storage_account_name"]}/{self.configLoader.config["SETTINGS"]["landing"]}/incoming/'
        df=self.spark.read.format('avro').load(path)
        df = df.withColumn("Body", col("Body").cast("string"))
        jsonOptions = {"dateFormat" : "yyyy-MM-dd HH:mm:ss.SSS"}
        df = df.withColumn("Body", from_json(df.Body,lnd_body_schema, jsonOptions))
        return df

    
    def __read_from_stg_raw(self):
        #read from staging raw
        try:
            from pyspark.sql.functions import col,explode_outer,lit,when
            path=f'dbfs:/mnt/{self.configLoader.config["SETTINGS"]["storage_account_name"]}/{self.configLoader.config["SETTINGS"]["staging"]}/raw/incoming/'
            #read from delta table. no read schem required
            df=self.spark.read.format('delta').load(path).filter(col('countryCode')==self.country_code)
            
            stg_max_load_date=self.max_load_date(df)
            tempdf=df.filter(col('load_date')==stg_max_load_date).select('Body')
            # Flatten Body
            for c in df.select('Body').schema['Body'].dataType:
                tempdf= tempdf.withColumn(c.name, col("Body." + c.name))
            
            processed_df=tempdf.drop('Body')

        except:
            raise IOError('failed to create staging work df')

        return processed_df
        
    def create_stg_raw_df(self,lnd_body_schema,lnd_df):
        #create df from landing to be loaded to stg raw table
        from pyspark.sql.functions import lit,asc
        raw_stg_df=self.read_df_from_landing(lnd_body_schema).withColumn('load_date_time',lit(datetime.datetime.now())).withColumn('load_date',lit(datetime.date.today()))\
            .withColumn('countryCode',lit(self.country_code))
        
        raw_stg_df=raw_stg_df.orderBy(asc('sequenceNumber'))
        #raw_stg_df.show()
        path=f'dbfs:/mnt/{self.configLoader.config["SETTINGS"]["storage_account_name"]}/{self.configLoader.config["SETTINGS"]["staging"]}/raw/incoming/'
        
        return raw_stg_df
    
    def create_stg_processed_df(self):
        #create df from staging raw to be loaded to staging processed
        from pyspark.sql.functions import lit
        processed_df=self.__read_from_stg_raw().withColumn('load_date_time',lit(datetime.datetime.now())).withColumn('load_date',lit(datetime.date.today()))\
            .withColumn('countryCode',lit(self.country_code))
        newpath=f'dbfs:/mnt/{self.configLoader.config["SETTINGS"]["storage_account_name"]}/{self.configLoader.config["SETTINGS"]["staging"]}/processed/incoming/'
        
        return processed_df
    
    def create_int_df_from_stg_processed(self):
        #create df from staging processed to be loaded to integration
        try:
            from pyspark.sql.functions import col,explode_outer,lit,when
            path=f'dbfs:/mnt/{self.configLoader.config["SETTINGS"]["storage_account_name"]}/{self.configLoader.config["SETTINGS"]["staging"]}/processed/incoming/'

            df=self.spark.read.format('delta').load(path).filter(col('countryCode')==self.country_code)
            
            stg_max_load_date_time=self.jsonExtract.max_load_date_time(df)
            
            temp=df.select('countryCode','load_date_time','load_date',explode_outer(df.items)).withColumnRenamed('col','items').filter(col('load_date_time')==stg_max_load_date_time)
            temp=temp.withColumn('ratingDisabled',when(temp.items.statistics.likeCount.isNull(),lit(True)).otherwise(False)).\
                withColumn('commentDisabled',when(temp.items.statistics.commentCount.isNull(),lit(True)).otherwise(False))
            
            #temp.select('countryCode','load_date_time','load_date','ratingDisabled','commentDisabled',temp.items.id,temp.items.statistics.commentCount,temp.items.statistics.dislikeCount,temp.items.statistics.favoriteCount,temp.items.statistics.likeCount,temp.items.statistics.viewCount,\
            #    temp.items.snippet.title,temp.items.snippet.publishedAt,temp.items.snippet.channelId,temp.items.snippet.channelTitle,temp.items.snippet.categoryId,temp.items.snippet.tags)\
            #    .createOrReplaceTempView('tempview')
            for c in temp.schema['items'].dataType:
                temp= temp.withColumn(c.name, col("items." + c.name))
            for c in temp.schema['snippet'].dataType:
                temp= temp.withColumn(c.name, col("snippet." + c.name))
            for c in temp.schema['statistics'].dataType:
                temp= temp.withColumn(c.name, col("statistics." + c.name))
            temp.drop('items').drop('snippet').drop('statistics').createOrReplaceTempView('tempview')
            

            intdf=self.spark.sql(eval(self.configLoader.config['SETTINGS']['features'])).withColumn('load_date_time',lit(datetime.datetime.now())).\
                withColumn('load_date',lit(datetime.date.today()))
            intdf_clean=self.jsonExtract.special_char_cleanup(intdf)
        except:
            self.logger.error('unable to create video df from staging')
            raise IOError ('unable to create video df from staging')
        
        return intdf_clean
