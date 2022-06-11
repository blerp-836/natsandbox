import requests, sys, time, os, argparse,datetime,json
from pathlib import Path
import subprocess
from adls.classes.ConfigLoader import ConfigLoader
from utilities import KeyVault,AdlsFs
from adls.classes.CustomSchema import CustomSchema

#get_data
    #get_pages
        #api_request
        #get_video
    #write_to_file

class jsonExtract():
    def __init__(self,dbutils,spark,configLoader,logger):
        self.configLoader=configLoader
        self.dbutils=dbutils
        #use dbutils when migrating to databricks
        self.api_key=self.dbutils.secrets.get(scope = "databricks-akv", key = self.configLoader.config['API']['API_KEY'])
        #self.keyVault=KeyVault.KeyVault()
        #self.api_key=self.keyVault.main(self.configLoader.config['API']['api_key'])
        self.adls_key=self.dbutils.secrets.get(scope='databricks-akv',key=self.configLoader.config['SETTINGS']['adls_key'])
        self.spark=spark
        self.request_url=eval(self.configLoader.config['API']['request_url'])
        self.country_code=self.configLoader.config['API']['country_code']
        self.next_page_token=eval(self.configLoader.config['API']['page_token'])
        self.incoming_file=configLoader.config['SETTINGS']['landing_file']
        self.adlsFs=AdlsFs.AdlsFs()
        self.logger=logger.logger


    def max_load_date_time(self,df):
        max_time_col=df.select(self.configLoader.config['DEST']['DELTA_COL']).groupBy().agg({self.configLoader.config['DEST']['DELTA_COL']:'max'}).collect()[0][0]
        return max_time_col
        
    def special_char_cleanup(self,df):
        # remove special characters except the one in the list
        from pyspark.sql.functions import when, lit,col,regexp_replace
        dfnosp=df
        for item in df.dtypes:
            if item[1]=='string':
                    #dfnosp.filter(col(item[0]).isNull()).select(item[0]).show()
                    dfnosp=dfnosp.withColumn(item[0],when(col(item[0]).isNotNull(),regexp_replace(item[0],'[^)(+_A-Za-z0-9 \'àâçéèêëîïôûùüÿñæœ&ÀÂÇÉÈÊËÎÏÔÛÙÜŸÑÆŒ.-]','')))
        return dfnosp

    def create_df_from_landing(self,schema=None):
        try:
            from pyspark.sql.functions import lit
            path=f'dbfs:/mnt/{self.configLoader.config["SETTINGS"]["storage_account_name"]}/{self.configLoader.config["SETTINGS"]["landing"]}/incoming/'
            self.logger.info(path)
            if schema!=None:
                tempdf=self.spark.read.schema(schema).option('multiline','true').json(path)
                df=tempdf.withColumn('countryCode',lit(self.country_code)).withColumn('load_date_time',lit(datetime.datetime.now())).withColumn('load_date',lit(datetime.date.today()))
            else: 
                tempdf=self.spark.read.option('multiline','true').json(path)
                df=tempdf.withColumn('countryCode',lit(self.country_code)).withColumn('load_date_time',lit(datetime.datetime.now())).withColumn('load_date',lit(datetime.date.today()))
            
            
        except:
            self.logger.error('failed to create and write df from landing to staging')
            raise IOError('failed to create and write df from landing to staging')
        
        return df
    
        
    def create_dim_df_from_stg(self):
        try:
            from pyspark.sql.functions import col,explode_outer,lit,when
            path=f'dbfs:/mnt/{self.configLoader.config["SETTINGS"]["storage_account_name"]}/{self.configLoader.config["SETTINGS"]["staging"]}/incoming/'

            df=self.spark.read.format('delta').load(path).filter(col('countryCode')==self.country_code)
            #print('getting max load date time')
            stg_max_load_date_time=self.max_load_date_time(df)
            #print(stg_max_load_date_time)
            temp=df.select('countryCode','load_date_time','load_date',explode_outer(df.items)).withColumnRenamed('col','items').filter(col('load_date_time')==stg_max_load_date_time)
            #temp.select('countryCode','load_date_time','load_date',temp.items['id'],temp.items['snippet']['assignable'],temp.items['snippet']['channelId'],\
            #temp.items['snippet']['title']).createOrReplaceTempView('tempview')
            for c in temp.schema['items'].dataType:
                temp= temp.withColumn(c.name, col("items." + c.name))
            for c in temp.schema['snippet'].dataType:
                temp= temp.withColumn(c.name, col("snippet." + c.name))
            temp.drop('items').drop('snippet').createOrReplaceTempView('tempview')
            #intdf=self.spark.sql(eval(self.configLoader.config['SETTINGS']['features'])).withColumn('load_date_time',lit(datetime.datetime.now()))
            intdf=self.spark.sql(eval(self.configLoader.config['SETTINGS']['features'])).withColumn('load_date_time',lit(datetime.datetime.now())).\
                withColumn('load_date',lit(datetime.date.today()))
            intdf_clean=self.special_char_cleanup(intdf)
        except:
            self.logger.error('unable to create video cat df from staging')
            raise IOError('unable to create video cat df from staging')

        return intdf_clean
   
