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
from adls.classes.jsonExtract import jsonExtract
from adls.classes.deltaLoad import deltaLoad
from adls.classes.deltaTransform import deltaTransform
from adls.classes.deltaExtract import deltaExtract
import xmlrunner
import json
import unittest

class delta_load_unittest(unittest.TestCase):
    def setUp(self):
        self.dir = os.path.dirname(os.path.abspath(__file__))
        self.read_schema=CustomSchema.lnd_ytb_i18nRegions_schema()
        self.job_name='ytb_i18nRegions'
        if not os.path.exists(self.dir+"/logs/"):
            os.makedirs(self.dir+"/logs/")
        self.path=self.dir+"/logs/{0}.log".format(self.job_name)
        self.timedlogger=FileLogging(self.job_name,'INFO',self.path)
        self.timedlogger.create_timed_rotating_log()
        self.configLoader=ConfigLoader(self.dir,self.job_name)
        self.configLoader.read_default_config()
        self.configLoader.read_job_config()
        
        self.spark = SparkSession.builder.getOrCreate()
        self.dbutils = DBUtils(self.spark)

    
    def test_append_to_dest_stg_df(self):
        
        tbl_name='stg_i18n_regions_df'
        print(tbl_name)
        self.configLoader.read_table_config(tbl_name)
        read_schema=CustomSchema.lnd_ytb_i18nRegions_schema()
        df=jsonExtract(self.dbutils,self.spark,self.configLoader,self.timedlogger).create_df_from_landing(self.read_schema)
        #delta_extract=deltaExtract(self.configLoader,self.timedlogger,self.spark,self.dbutils)
        
        delta_transform=deltaTransform(self.configLoader,self.timedlogger,self.spark,self.dbutils,read_schema,df)
        
        delta_load=deltaLoad(self.configLoader,self.timedlogger,self.spark,self.dbutils,read_schema,df)
        
        res=delta_load.append_to_dest()
        self.assertTrue(res==0)
    
    def test_append_to_dest_int_df(self):
        
        tbl_name='int_i18n_regions_metastore'
        print(tbl_name)
        self.configLoader.read_table_config(tbl_name)
        #delta_extract=deltaExtract(self.configLoader,self.timedlogger,self.spark,self.dbutils)
        
        stg_df=jsonExtract(self.dbutils,self.spark,self.configLoader,self.timedlogger).create_dim_df_from_stg()
        delta_transform=deltaTransform(self.configLoader,self.timedlogger,self.spark,self.dbutils,read_schema=None,df=stg_df)
        
        delta_load=deltaLoad(self.configLoader,self.timedlogger,self.spark,self.dbutils,read_schema=None,df=stg_df)
        
        res=delta_load.append_to_dest()
        self.assertTrue(res==0)


if __name__ == "__main__":
    def run_tests():
        test_classes_to_run=[delta_load_unittest]
        loader=unittest.TestLoader()
        suites_list=[]
        for test_class in test_classes_to_run:
            suite=loader.loadTestsFromTestCase(test_class)
            suites_list.append(suite)
        all_suite=unittest.TestSuite(suites_list)
        runner=xmlrunner.XMLTestRunner(output=os.getcwd()+'/test/')
        runner.run(all_suite)

# COMMAND ----------

    run_tests()