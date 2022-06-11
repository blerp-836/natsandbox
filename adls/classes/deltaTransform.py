from pyspark.sql.functions import col,lit,hash,concat
from adls.classes.deltaExtract import deltaExtract
import pandas as pd
import datetime
import os

class deltaTransform():
    def __init__(self,configLoader,logger,spark,dbutils,read_schema=None,df=None):
        self.configLoader=configLoader
        self.logger=logger.logger
        self.spark=spark
        self.dbutils=dbutils
        self.deltaExtract=deltaExtract(self.configLoader,logger,self.spark,self.dbutils)
        self.deltaExtract.get_df_source(read_schema,df)
        self.df_source=self.deltaExtract.df_source
        self.deltaExtract.get_df_dest()
        self.df_dest=self.deltaExtract.df_dest
        self.dest_delta_table=self.deltaExtract.get_dest_delta_table()
        
    
    def get_source_pk(self):
        source_pk_code=eval(self.configLoader.config['SOURCE']['SOURCE_PK'])
        self.df_source=self.df_source.withColumn(source_pk_code[0],\
            hash(concat(col(source_pk_code[1][0]),col(source_pk_code[1][1]))))
        return 0

    def read_mapping_csv(self,table_name):
        print(self.configLoader.config['MAPPING']['PATH']+table_name)
        try:
            self.mapping_pdf=pd.read_csv(os.getcwd()+'/'+self.configLoader.config['MAPPING']['PATH']+table_name+'.csv')
        except FileNotFoundError:
            try:
                self.mapping_pdf=pd.read_csv('/'+self.configLoader.config['MAPPING']['PATH']+table_name+'.csv')
            except:
                raise IOError
        except:
            raise IOError


            
        return 0

    def join_to_fk_table (self):
        mapping_dict=eval(self.configLoader.config['MAPPING']['FK_JOIN'])
        
        for fk_table, cols in mapping_dict.items():
            source_cols=cols[0]
            foreign_cols=cols[1]
            foreign_query=eval(self.configLoader.config['MAPPING']['FOREIGN_QUERY'])[fk_table]
            df_foreign=self.spark.sql(foreign_query)
            if self.configLoader.config['MAPPING']['SCD_TYPE']=='2':
                self.df_source=self.df_source.join(df_foreign,[*[self.df_source[source_col] == df_foreign[foreign_col] for (source_col, foreign_col) in zip(source_cols, foreign_cols)]\
                    ,self.df_source[self.configLoader.config['SOURCE']['SOURCE_TS']]>=df_foreign[self.configLoader.config['MAPPING']['EFF_START_DT']],self.df_source[self.configLoader.config['SOURCE']['SOURCE_TS']]<=df_foreign[self.configLoader.config['MAPPING']['EFF_END_DT']]],how='leftouter')
            else:
                self.df_source=self.df_source.join(df_foreign,[self.df_source[source_col] == df_foreign[foreign_col] for (source_col, foreign_col) in zip(source_cols, foreign_cols)],how='leftouter')
        
        return 0
    

    def write_df_source_to_temp_table(self):
        self.df_source.write.format('delta').mode('overwrite').save(self.configLoader.config['SOURCE']['TEMP_PATH'])
        return 0
    
    def get_df_source_from_temp_table(self):
        self.spark.catalog.clearCache()
        self.df_source=self.spark.read.format('delta').load(self.configLoader.config['SOURCE']['TEMP_PATH'])
        return 0

    def rename_source_to_dest(self):
        temp_pdf=self.mapping_pdf[self.mapping_pdf['target'].notna()]
        
        for ind,rows in temp_pdf.iterrows():
            self.df_source=self.df_source.withColumnRenamed(rows['source'],rows['target'])
        #self.df_source.show()
        return 0
    
    def add_metadata_cols(self):
        self.df_source=self.df_source.withColumn(self.configLoader.config['DEST']['MODIFIED_TS'],lit(datetime.datetime.now()))

        return 0
    
    def select_dest_cols(self):
        dest_cols=eval(self.configLoader.config['DEST']['DEST_COLS'])
        #print(*dest_cols)
        self.df_source=self.df_source.select(*dest_cols)



    
