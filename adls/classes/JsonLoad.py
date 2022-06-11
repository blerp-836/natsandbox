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

class JsonLoad():
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



    def __special_char_cleanup(self,df):
        # remove special characters except the one in the list
        from pyspark.sql.functions import when, lit,col,regexp_replace
        dfnosp=df
        for item in df.dtypes:
            if item[1]=='string':
                    #dfnosp.filter(col(item[0]).isNull()).select(item[0]).show()
                    dfnosp=dfnosp.withColumn(item[0],when(col(item[0]).isNotNull(),regexp_replace(item[0],'[^)(+_A-Za-z0-9 \'àâçéèêëîïôûùüÿñæœ&ÀÂÇÉÈÊËÎÏÔÛÙÜŸÑÆŒ.-]','')))
        return dfnosp

    def __create_df_from_landing(self,key1,key2,zone,schema=None):
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
            self.__write_to_parquet(df,zone,key1,key2)
            
        except:
            self.logger.error('failed to create and write df from landing to staging')
            raise IOError('failed to create and write df from landing to staging')
        return 0
   
    def max_load_date_time(self,df):
        max_time_col=df.select(self.configLoader.config['API']['key2']).groupBy().agg({self.configLoader.config['API']['key2']:'max'}).collect()[0][0]
        return max_time_col

    def __create_video_df_from_stg(self,read_schema):
        try:
            from pyspark.sql.functions import col,explode_outer,lit,when
            path=f'dbfs:/mnt/{self.configLoader.config["SETTINGS"]["storage_account_name"]}/{self.configLoader.config["SETTINGS"]["staging"]}/processed/incoming/'
            if read_schema!=None:
                df=self.spark.read.schema(read_schema).format('parquet').load(path).filter(col('countryCode')==self.country_code)
            else:
                df=self.spark.read.format('parquet').load(path).filter(col('countryCode')==self.country_code)
            
            stg_max_load_date_time=self.max_load_date_time(df)
            
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
            intdf_clean=self.__special_char_cleanup(intdf)
        except:
            self.logger.error('unable to create video df from staging')
            raise IOError ('unable to create video df from staging')
        
        return intdf_clean

    
    def __create_dim_df_from_stg(self,read_schema):
        try:
            from pyspark.sql.functions import col,explode_outer,lit,when
            path=f'dbfs:/mnt/{self.configLoader.config["SETTINGS"]["storage_account_name"]}/{self.configLoader.config["SETTINGS"]["staging"]}/incoming/'
            if read_schema!=None:
                df=self.spark.read.schema(read_schema).format('parquet').load(path).filter(col('countryCode')==self.country_code)
            else:
                df=self.spark.read.format('parquet').load(path).filter(col('countryCode')==self.country_code)
            
            stg_max_load_date_time=self.max_load_date_time(df)
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
            intdf_clean=self.__special_char_cleanup(intdf)
        except:
            self.logger.error('unable to create video cat df from staging')
            raise IOError('unable to create video cat df from staging')

        return intdf_clean
##depecreated. no longer in use###
#    def __create_regions_df_from_stg(self,read_schema):
#        try:
#            from pyspark.sql.functions import col,explode_outer,lit,when
#            path=f'dbfs:/mnt/{self.configLoader.config["SETTINGS"]["storage_account_name"]}/{self.configLoader.config["SETTINGS"]["staging"]}/incoming/'
#            if read_schema!=None:
#                df=self.spark.read.schema(read_schema).format('parquet').load(path).filter(col('countryCode')==self.country_code)
#            else:
#                df=self.spark.read.format('parquet').load(path).filter(col('countryCode')==self.country_code)
#            
#            
#            stg_max_date=self.max_load_date_time(df)
#            temp=df.select('countryCode','load_date_time','load_date',explode_outer(df.items)).withColumnRenamed('col','items').filter(col('load_date')==stg_max_date)
#            temp.select('countryCode','load_date_time','load_date',temp.items['id'],temp.items['snippet']['gl'],temp.items['snippet']['name']).createOrReplaceTempView('tempview')
#
#            intdf=self.spark.sql(eval(self.configLoader.config['SETTINGS']['features'])).withColumn('load_date_time',lit(datetime.datetime.now())).\
#                withColumn('load_date',lit(datetime.date.today()))
#            
#            intdf_clean=self.__special_char_cleanup(intdf)
#        except:
#            self.logger.error('unable to create regionis df from staging')
#            raise IOError('unable to create regions df from staging')
        

        return intdf_clean
        
    def __write_to_parquet(self,DF,zone,key1,key2,default=True):
        storage_account_name=self.configLoader.config['SETTINGS']['storage_account_name']
        container_name=self.configLoader.config['SETTINGS'][zone]            
        path=f'dbfs:/mnt/{storage_account_name}/{container_name}/incoming/'
        if default:
            DF.write.mode('append').partitionBy(key1,key2).format('parquet').save(path)
        else:
            num_part=eval(self.configLoader.config['SETTINGS']['num_part'])
            max_records_num=eval(self.configLoader.config['SETTINGS']['max_records_num'])
            DF.coalesce(num_part).write.mode('append').option('maxRecordsPerFile',max_records_num).partitionBy(key1,key2).format('parquet').save(path)
        return 0
    def staging_load(self,read_schema):
        self.__create_df_from_landing(self.configLoader.config['API']['key1'],self.configLoader.config['API']['key2'],'staging',read_schema)
        return 0

    def int_load(self,read_schema,api_type):
        if api_type=='video':
            DF=self.__create_video_df_from_stg(read_schema)
            DF.printSchema()
            self.__write_to_parquet(DF,'integration',self.configLoader.config['API']['key1'],self.configLoader.config['API']['key2'])
        elif api_type=='videoCat':
            DF=self.__create_dim_df_from_stg(read_schema)
            DF.printSchema()
            self.__write_to_parquet(DF,'integration',self.configLoader.config['API']['key1'],self.configLoader.config['API']['key2'])
        elif api_type=='i18nRegions':
            DF=self.__create_dim_df_from_stg(read_schema)
            DF.printSchema()
            self.__write_to_parquet(DF,'integration',self.configLoader.config['API']['key1'],self.configLoader.config['API']['key2'])
        return 0
    
            

            

        


        
    