#from delta.classes.ConfigLoader import ConfigLoader
from pyspark.sql.functions import col,max, hash
from datetime import datetime

class deltaLoad():
    def __init__(self,configLoader,logger,spark,dbutils,df_source=None,df_dest=None,dest_delta_table=None):
        self.configLoader=configLoader
        self.logger=logger.logger
        self.spark=spark
        self.dbutils=dbutils
        self.delta_col=self.configLoader.config['DEST']['DELTA_COL']
        self.src_delta_col=self.configLoader.config['SOURCE']['DELTA_COL']
        self.upsert_statement=self.configLoader.config['DEST']['UPSERT_STATEMENT']
        self.update_dict=self.configLoader.config['DEST']['UPDATE_DICT']       
        self.insert_dict=self.configLoader.config['DEST']['INSERT_DICT']
        self.df_dest=df_dest
        self.df_source=df_source
        self.dest_delta_table=dest_delta_table
        self.part_keys=eval(self.configLoader.config['DEST']['PART_KEYS'])
        #print(self.part_keys)
    
    def __upsert_to_dest_sql(self):
        self.df_source.createOrReplaceTempView('df_source_view')
        self.spark.sql('{upsert_statement}'.\
            format(upsert_statement=eval(self.upsert_statement)))
        return 0
    
    def __upsert_to_dest_py(self):
        
        self.dest_delta_table.alias("dest").merge(self.df_source.alias("source"),\
            "dest.{dest_pk} = source.{source_pk}".\
            format(dest_pk=self.configLoader.config['DEST']['DEST_PK'],\
            source_pk=eval(self.configLoader.config['SOURCE']['SOURCE_PK'])[0]))\
            .whenMatchedUpdate(set=eval(self.update_dict))\
            .whenNotMatchedInsert(values=eval(self.insert_dict))\
            .execute()
        return 0

    def upsert_to_dest(self):
        if eval(self.upsert_statement)!='':
            assert eval(self.update_dict)=={} and eval(self.insert_dict)=={}, 'can only use one method'
            self.__upsert_to_dest_sql()
        else:
            assert eval(self.upsert_statement)=='', 'can only use one method'                                 
            self.__upsert_to_dest_py()
        return 0
            
        
    def append_to_dest(self,df=None):
        if self.configLoader.config['DEST']['DEDUPLICATION'].lower()=='true':
            self.dest_delta_table.alias("dest").merge(self.df_source.alias("source"),\
                "dest.{dest_pk} = source.{source_pk}".\
                format(dest_pk=self.configLoader.config['DEST']['DEST_PK'],\
                source_pk=eval(self.configLoader.config['SOURCE']['SOURCE_PK'])[0]))\
                .whenNotMatchedInsertAll()\
                .execute()
        else:
            
            self.df_source.write.format('delta').mode('append').option('path',self.configLoader.config['DEST']['DEST_PATH']).partitionBy(self.part_keys[0],self.part_keys[1]).\
                saveAsTable('{0}.{1}'.format(self.configLoader.config['DEST']['DEST_SCHEMA'],self.configLoader.config['DEST']['DEST_TABLE']))
        return 0

    def scd1_load(self):
        scd1_cond=eval(self.configLoader.config['DEST']['UPDATE_COND'])
        self.dest_delta_table.alias("dest").merge(self.df_source.alias("source"),\
            "dest.{dest_pk} = source.{source_pk}".\
            format(dest_pk=self.configLoader.config['DEST']['DEST_PK'],\
            source_pk=eval(self.configLoader.config['SOURCE']['SOURCE_PK'])[0]))\
            .whenMatchedUpdate(condition=scd1_cond,set =eval(self.update_dict)) \
            .whenNotMatchedInsert(values =eval(self.insert_dict))\
            .execute()
        return 0
    
    def scd2_load(self):
        scd2_update_cond=eval(self.configLoader.config['DEST']['UPDATE_COND'])
        scd2_insert_cond=eval(self.configLoader.config['DEST']['SCD2_INSERT_COND'])
        insert_df=self.df_source.alias('source').join(self.df_dest.alias('dest'),\
            self.df_dest[self.configLoader.config['DEST']['DEST_PK']]==self.df_dest[eval(self.configLoader.config['SOURCE']['SOURCE_PK'])[0]]).\
            where(scd2_insert_cond)
        staged_updates_df=insert_df.selectExpr('NULL as mergekey','source.*').union(self.df_source.select(col(eval(self.configLoader.config['SOURCE']['SOURCE_PK'])[0]).alias('mergekey'),'*'))
        
        self.dest_delta_table.alias('dest').merge(staged_updates_df.alias('staged_updates'),'dest.{0}=mergekey'.format(self.configLoader.config['DEST']['DEST_PK']))\
        .whenMatchedUpdate(condition=scd2_update_cond,set=eval(self.update_dict))\
        .whenNotMatchedInsert(values=eval(self.insert_dict)).\
        execute()
        return 0

    def delete_dest(self):
        print(eval(self.configLoader.config['DEST']['DEL_COND']))
        self.dest_delta_table.delete(eval(self.configLoader.config['DEST']['DEL_COND']))
        return 0
    
    def create_delta_table(self):
        try:
            table_name=self.configLoader.config['DEST']['DEST_SCHEMA'],self.configLoader.config['DEST']['DEST_TABLE']
            save_path=self.configLoader.config['DEST']['DEST_PATH']
            self.spark.sql(f"CREATE TABLE IF NOT EXISTS " + {table_name} + " USING DELTA LOCATION '" + {save_path} + "'")
            self.logger.info(f'{table_name} table created in hive metastore')
        except:
            self.logger.error(f'{table_name} failed to create table in hive metastore')
            raise IOError
        return 0