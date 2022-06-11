#from delta.classes.ConfigLoader import ConfigLoader
from pyspark.sql.functions import col,max, hash,concat
from datetime import datetime
from delta.tables import *

class deltaExtract():
    def __init__(self,configLoader,logger,spark,dbutils):
        self.configLoader=configLoader
        self.logger=logger.logger
        self.spark=spark
        self.dbutils=dbutils
        self.delta_col=self.configLoader.config['DEST']['DELTA_COL']
        self.src_delta_col=self.configLoader.config['SOURCE']['DELTA_COL']

    def get_df_source_dbfs(self,read_schema):
        
        file_format=self.configLoader.config['SOURCE']['FILE_FORMAT']
        if DeltaTable.isDeltaTable(self.spark,self.configLoader.config['SOURCE']['SOURCE_PATH']):
            self.df_source=self.spark.read.format(file_format).\
                    load(self.configLoader.config['SOURCE']['SOURCE_PATH']).\
                    filter(col(self.src_delta_col)>self.max_ts)
        else:
            self.df_source=self.spark.read.schema(read_schema).format(file_format).\
                    load(self.configLoader.config['SOURCE']['SOURCE_PATH']).\
                    filter(col(self.src_delta_col)>self.max_ts)
        
        return 0

    def get_max_ts(self):
        self.get_df_dest()
        self.max_ts=self.df_dest.select(max(self.delta_col)).first()[0]
        self.max_ts=datetime.strptime('1970-01-01 00:00:00','%Y-%m-%d %H:%M:%S')\
             if self.max_ts==None else self.max_ts
        
        return 0

    def get_df_source_table(self):
        source_query=eval(self.configLoader.config['SOURCE']['SOURCE_QUERY']).\
            format(self.configLoader.config['SOURCE']['SOURCE_SCHEMA'],\
            self.configLoader.config['SOURCE']['SOURCE_TABLE'])
        self.df_source=self.spark.sql(source_query.format(max_ts={self.max_ts}))     
        
        return 0
    
    def get_df_source(self,read_schema=None,df=None):
        self.get_max_ts()
        if self.configLoader.config['SOURCE']['SOURCE_TYPE'].lower()=='metastore':
            self.get_df_source_table()
            
        elif self.configLoader.config['SOURCE']['SOURCE_TYPE'].lower()=='dbfs':
            self.get_df_source_dbfs(read_schema)
            
        elif self.configLoader.config['SOURCE']['SOURCE_TYPE'].lower()=='delta':
            self.df_source=self.get_dest_delta_table()
        elif self.configLoader.config['SOURCE']['SOURCE_TYPE'].lower()=='dataframe':
            self.df_source=df
        #self.df_source.show()
        return 0

    def get_df_dest(self):
        if self.configLoader.config['DEST']['DEST_TYPE'].lower()=='delta':
            print(self.configLoader.config['DEST']['DEST_PATH'])
            self.dest_delta_table=self.get_dest_delta_table()
            
        elif self.configLoader.config['DEST']['DEST_TYPE'].lower()=='dataframe':
            self.get_dest_delta_df()
        return 0
    
    def get_dest_delta_table(self):
        #return delta table not df
        deltaTable=DeltaTable.forPath(self.spark,self.configLoader.config['DEST']['DEST_PATH'])
        return deltaTable


    def get_dest_delta_df(self):
        dest_query=eval(self.configLoader.config['DEST']['DEST_QUERY']).\
            format(self.configLoader.config['DEST']['DEST_SCHEMA'],\
            self.configLoader.config['DEST']['DEST_TABLE'])
        self.df_dest=self.spark.sql(dest_query)     
        
        return 0
        
