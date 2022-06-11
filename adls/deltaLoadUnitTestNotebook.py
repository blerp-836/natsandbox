# Databricks notebook source
from pathlib import Path
import os
TOP_DIR = Path(os.getcwd()).resolve().parent
import sys
sys.path.append(str(TOP_DIR))
print(sys.path)
import datetime
import argparse
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from utilities.FileLogging import FileLogging
from adls.classes.CustomSchema import CustomSchema
from adls.classes.ConfigLoader import ConfigLoader
from adls.classes.jsonExtract import jsonExtract
from adls.classes.deltaLoad import deltaLoad
from adls.classes.deltaTransform import deltaTransform
from adls.classes.deltaExtract import deltaExtract
import xmlrunner
import json
import unittest

class delta_load_unittest(unittest.TestCase):
    def setUp(self):
        self.dir = os.path.abspath(os.getcwd())
        self.read_schema=CustomSchema.lnd_ytb_i18nRegions_schema()
        self.job_name='ytb_i18nRegions'
        
        self.path=f'/tmp/{self.job_name}.log'
        self.timedlogger=FileLogging(self.job_name,'INFO',self.path)
        self.timedlogger.create_timed_rotating_log()
        self.configLoader=ConfigLoader(self.dir,self.job_name)
        self.configLoader.read_default_config()
        self.configLoader.read_job_config()
        
        self.spark = SparkSession.builder.getOrCreate()
        self.dbutils = DBUtils(self.spark)
        #dbutils.notebook.run('deltaExtractUnitTestNotebook',0)
        

    
    def append_to_dest_stg_df(self):
        
        tbl_name='stg_i18n_regions_df'
        print('==================================={0}========================'.format(tbl_name))
        self.configLoader.read_table_config(tbl_name)
        read_schema=CustomSchema.lnd_ytb_i18nRegions_schema()
        df=jsonExtract(self.dbutils,self.spark,self.configLoader,self.timedlogger).create_df_from_landing(self.read_schema)
        #delta_extract=deltaExtract(self.configLoader,self.timedlogger,self.spark,self.dbutils)
        
        delta_transform=deltaTransform(self.configLoader,self.timedlogger,self.spark,self.dbutils,read_schema,df)
        
        delta_load=deltaLoad(self.configLoader,self.timedlogger,self.spark,self.dbutils,df_source=delta_transform.df_source,df_dest=delta_transform.df_dest)
        
        res=delta_load.append_to_dest()
        self.assertTrue(res==0)
    
    def append_to_dest_int_df(self):

        self.append_to_dest_stg_df()
        tbl_name='int_i18n_regions_df'
        print('==================================={0}========================'.format(tbl_name))
        self.configLoader.read_table_config(tbl_name)
        #delta_extract=deltaExtract(self.configLoader,self.timedlogger,self.spark,self.dbutils)
        
        stg_df=jsonExtract(self.dbutils,self.spark,self.configLoader,self.timedlogger).create_dim_df_from_stg()
        delta_transform=deltaTransform(self.configLoader,self.timedlogger,self.spark,self.dbutils,read_schema=None,df=stg_df)

        delta_load=deltaLoad(self.configLoader,self.timedlogger,self.spark,self.dbutils,df_source=delta_transform.df_source,df_dest=delta_transform.df_dest)
        
        res=delta_load.append_to_dest()
        self.assertTrue(res==0)
    
    def append_to_dest_pst(self):
        self.append_to_dest_int_df()
        tbl_name='pst_i18n_regions_metastore'
        print('==================================={0}========================'.format(tbl_name))
        self.configLoader.read_table_config(tbl_name)
        
        delta_transform=deltaTransform(self.configLoader,self.timedlogger,self.spark,self.dbutils)
        
        delta_transform.read_mapping_csv(tbl_name)
        delta_transform.rename_source_to_dest()
        delta_transform.add_metadata_cols()
        delta_transform.get_source_pk()
        int_df=delta_transform.df_source
        pst_df=delta_transform.df_dest
        
        delta_load=deltaLoad(self.configLoader,self.timedlogger,self.spark,self.dbutils,df_source=int_df,df_dest=pst_df)

        res=delta_load.append_to_dest()
        self.assertTrue(res==0)
    
    def append_to_dest_pst_dedup(self):
        
        #self.append_to_dest_int_df()
        tbl_name='pst_i18n_regions_metastore_dedup'
        print('==================================={0}========================'.format(tbl_name))
        self.configLoader.read_table_config(tbl_name)
        
        delta_transform=deltaTransform(self.configLoader,self.timedlogger,self.spark,self.dbutils)
        
        delta_transform.read_mapping_csv(tbl_name)
        delta_transform.rename_source_to_dest()
        delta_transform.add_metadata_cols()
        delta_transform.get_source_pk()
        int_df=delta_transform.df_source
        pst_df=delta_transform.df_dest
        pst_dest_delta_table=delta_transform.dest_delta_table
        delta_load=deltaLoad(self.configLoader,self.timedlogger,self.spark,self.dbutils,df_source=int_df,df_dest=pst_df,dest_delta_table=pst_dest_delta_table)
        res=delta_load.append_to_dest()
        self.assertTrue(res==0)
    
    def upsert_to_dest(self):
        #self.append_to_dest_int_df()
        tbl_name='pst_i18n_regions_metastore_dedup'
        
        print('==================================={0}========================'.format(tbl_name))
        self.configLoader.read_table_config(tbl_name)
        
        delta_transform=deltaTransform(self.configLoader,self.timedlogger,self.spark,self.dbutils)
        
        delta_transform.read_mapping_csv(tbl_name)
        delta_transform.rename_source_to_dest()
        delta_transform.add_metadata_cols()
        delta_transform.get_source_pk()
        int_df=delta_transform.df_source.distinct()
        pst_df=delta_transform.df_dest
        pst_dest_delta_table=delta_transform.dest_delta_table
        int_df.show()
        delta_load=deltaLoad(self.configLoader,self.timedlogger,self.spark,self.dbutils,df_source=int_df,df_dest=pst_df,dest_delta_table=pst_dest_delta_table)
        
        res=delta_load.upsert_to_dest()
        
        self.assertTrue(res==0)
    
    def upsert_to_dest_py(self):
        #self.append_to_dest_int_df()
        tbl_name='pst_i18n_regions_metastore_dedup'
        
        print('==================================={0}========================'.format(tbl_name))
        self.configLoader.read_table_config(tbl_name)
        
        delta_transform=deltaTransform(self.configLoader,self.timedlogger,self.spark,self.dbutils)
        
        delta_transform.read_mapping_csv(tbl_name)
        delta_transform.rename_source_to_dest()
        delta_transform.add_metadata_cols()
        delta_transform.get_source_pk()
        int_df=delta_transform.df_source.distinct()
        pst_df=delta_transform.df_dest
        pst_dest_delta_table=delta_transform.dest_delta_table
        delta_load=deltaLoad(self.configLoader,self.timedlogger,self.spark,self.dbutils,df_source=int_df,df_dest=pst_df,dest_delta_table=pst_dest_delta_table)
        
        res=delta_load.upsert_to_dest()
        
        self.assertTrue(res==0)
    
    def test_scd1(self):
        self.append_to_dest_int_df()
        tbl_name='pst_i18n_regions_metastore_dedup'
        
        print('==================================={0}========================'.format(tbl_name))
        self.configLoader.read_table_config(tbl_name)
        
        delta_transform=deltaTransform(self.configLoader,self.timedlogger,self.spark,self.dbutils)
        
        delta_transform.read_mapping_csv(tbl_name)
        delta_transform.rename_source_to_dest()
        delta_transform.add_metadata_cols()
        delta_transform.get_source_pk()
        int_df=delta_transform.df_source.distinct()
        pst_df=delta_transform.df_dest
        pst_dest_delta_table=delta_transform.dest_delta_table
        delta_load=deltaLoad(self.configLoader,self.timedlogger,self.spark,self.dbutils,df_source=int_df,df_dest=pst_df,dest_delta_table=pst_dest_delta_table)
        
        res=delta_load.scd1_load()
        
        self.assertTrue(res==0)
        
    def scd2(self):
        #self.append_to_dest_int_df()
        tbl_name='pst_i18n_regions_metastore_scd2'
        
        print('==================================={0}========================'.format(tbl_name))
        self.configLoader.read_table_config(tbl_name)
        
        delta_transform=deltaTransform(self.configLoader,self.timedlogger,self.spark,self.dbutils)
        
        delta_transform.read_mapping_csv(tbl_name)
        delta_transform.rename_source_to_dest()
        delta_transform.add_metadata_cols()
        delta_transform.get_source_pk()
        int_df=delta_transform.df_source.distinct()
        pst_df=delta_transform.df_dest
        pst_dest_delta_table=delta_transform.dest_delta_table
        delta_load=deltaLoad(self.configLoader,self.timedlogger,self.spark,self.dbutils,df_source=int_df,df_dest=pst_df,dest_delta_table=pst_dest_delta_table)
        
        res=delta_load.scd2_load()
        
        self.assertTrue(res==0)
        
        
                              
  


# COMMAND ----------

def run_tests():
    test_classes_to_run=[delta_load_unittest]
    loader=unittest.TestLoader()
    suites_list=[]
    for test_class in test_classes_to_run:
        suite=loader.loadTestsFromTestCase(test_class)
        suites_list.append(suite)
    all_suite=unittest.TestSuite(suites_list)
    runner=xmlrunner.XMLTestRunner(output='/dbfs/mnt/deployblob123/deploy/')
    runner.run(all_suite)

# COMMAND ----------

run_tests()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*),count(distinct(*)) from ytb_analytics_int.int_i18n_regions

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*),count(distinct(*)) from ytb_analytics_pst.pst_i18n_regions

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct modified_Ts from ytb_analytics_pst.pst_i18n_regions
