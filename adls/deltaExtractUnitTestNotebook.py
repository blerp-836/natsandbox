# Databricks notebook source
# MAGIC %run Repos/natalia.theodora@accenture.com/sandbox/DDL/delta

# COMMAND ----------

import os
from pathlib import Path
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
from delta.tables import *

import json
import unittest

blob_account_name='deployblob123'
container_name='deploy'
class delta_extract_unittest(unittest.TestCase):
    def setUp(self):
        self.read_schema=CustomSchema.lnd_ytb_i18nRegions_schema()
        self.job_name='ytb_i18nRegions'
        self.path = f'/tmp/{self.job_name}.log'
        self.dir=os.path.abspath(os.getcwd())
        self.timedlogger=FileLogging(self.job_name,'INFO',self.path)
        self.timedlogger.create_timed_rotating_log()
        self.configLoader=ConfigLoader(self.dir,self.job_name)
        self.configLoader.read_default_config()
        self.configLoader.read_job_config()
        
        self.spark = SparkSession.builder.getOrCreate()
        self.dbutils = DBUtils(self.spark)
    
    def test_delta_get_delta_df(self):
        tbl_name='stg_i18n_regions_df'
        self.configLoader.read_table_config(tbl_name)
        delta_extract=deltaExtract(self.configLoader,self.timedlogger,self.spark,self.dbutils)
        res=delta_extract.get_df_dest()
        delta_extract.df_dest.show()
        self.assertTrue(res==0)
    
    def test_get_max_ts(self):
        tbl_name='stg_i18n_regions_df'
        self.configLoader.read_table_config(tbl_name)
        delta_extract=deltaExtract(self.configLoader,self.timedlogger,self.spark,self.dbutils)
        res=delta_extract.get_max_ts()
        print(delta_extract.max_ts)
        self.assertTrue(res==0)

    def test_delta_get_df_source_df(self):
        tbl_name='stg_i18n_regions_df'
        self.configLoader.read_table_config(tbl_name)
        read_schema=CustomSchema.lnd_ytb_i18nRegions_schema()
        df=jsonExtract(self.dbutils,self.spark,self.configLoader,self.timedlogger).create_df_from_landing(self.read_schema)
        delta_extract=deltaExtract(self.configLoader,self.timedlogger,self.spark,self.dbutils)
        res=delta_extract.get_df_source(read_schema,df)
        delta_extract.df_source.show()
        self.assertTrue(res==0)
    
    def test_delta_get_df_source_metastore(self):
        tbl_name='int_i18n_regions_metastore'
        self.configLoader.read_table_config(tbl_name)
        read_schema=CustomSchema.lnd_ytb_i18nRegions_schema()
        delta_extract=deltaExtract(self.configLoader,self.timedlogger,self.spark,self.dbutils)
        res=delta_extract.get_df_source(read_schema)
        delta_extract.df_source.show()
        self.assertTrue(res==0)
    
    def test_delta_get_df_source_dbfs(self):
        tbl_name='int_i18n_regions_dbfs'
        self.configLoader.read_table_config(tbl_name)
        read_schema=CustomSchema.lnd_ytb_i18nRegions_schema()
        delta_extract=deltaExtract(self.configLoader,self.timedlogger,self.spark,self.dbutils)
        res=delta_extract.get_df_source(read_schema)
        delta_extract.df_source.show()
        self.assertTrue(res==0)
    
    def test_delta_get_df_delta_table(self):
        tbl_name='int_i18n_regions_delta'
        self.configLoader.read_table_config(tbl_name)
        read_schema=CustomSchema.lnd_ytb_i18nRegions_schema()
        delta_extract=deltaExtract(self.configLoader,self.timedlogger,self.spark,self.dbutils)
        res=delta_extract.get_df_source(read_schema)
        
        self.assertTrue(res==0)

    
    def test_get_dest_delta_df(self):
        tbl_name='stg_i18n_regions_df'
        self.configLoader.read_table_config(tbl_name)
        read_schema=CustomSchema.lnd_ytb_i18nRegions_schema()
        delta_extract=deltaExtract(self.configLoader,self.timedlogger,self.spark,self.dbutils)
        res=delta_extract.get_dest_delta_df()
        print(delta_extract.df_dest.count())
        self.assertTrue(res==0)

    def test_get_dest_delta_table(self):
        tbl_name='stg_i18n_regions_df'
        self.configLoader.read_table_config(tbl_name)
        read_schema=CustomSchema.lnd_ytb_i18nRegions_schema()
        delta_extract=deltaExtract(self.configLoader,self.timedlogger,self.spark,self.dbutils)
        res=delta_extract.get_dest_delta_table()
        self.assertTrue(res!=None)
        
          
        

    def tearDown(self):
        pass


# COMMAND ----------

import xmlrunner
def run_tests():
    test_classes_to_run=[delta_extract_unittest]
    loader=unittest.TestLoader()
    suites_list=[]
    for test_class in test_classes_to_run:
        suite=loader.loadTestsFromTestCase(test_class)
        suites_list.append(suite)
    all_suite=unittest.TestSuite(suites_list)
    runner=xmlrunner.XMLTestRunner(output='/dbfs/mnt/deployblob123/deploy')
    runner.run(all_suite)

# COMMAND ----------

run_tests()
