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

import json
import unittest

class delta_transform_unittest(unittest.TestCase):
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

    def test_read_mapping_csv(self):
        tbl_name='pst_i18n_regions_metastore'
        self.configLoader.read_table_config(tbl_name)
        delta_transform=deltaTransform(self.configLoader,self.timedlogger,self.spark,self.dbutils)
        res=delta_transform.read_mapping_csv(tbl_name)
        print(delta_transform.mapping_pdf)
        self.assertTrue(res==0)
        
    
    def test_rename_source_to_dest(self):
        tbl_name='pst_i18n_regions_metastore'
        self.configLoader.read_table_config(tbl_name)
        delta_transform=deltaTransform(self.configLoader,self.timedlogger,self.spark,self.dbutils)
        delta_transform.read_mapping_csv(tbl_name)
        res=delta_transform.rename_source_to_dest()
        print(res)
        self.assertTrue(res==0)
    
    def test_write_df_source_to_temp_table(self):
        tbl_name='pst_i18n_regions_metastore'
        self.configLoader.read_table_config(tbl_name)
        delta_transform=deltaTransform(self.configLoader,self.timedlogger,self.spark,self.dbutils)
        res=delta_transform.write_df_source_to_temp_table()
        print(res)
        self.assertTrue(res==0)
        

    def test_get_df_source_from_temp_table(self):
        tbl_name='pst_i18n_regions_metastore'
        self.configLoader.read_table_config(tbl_name)
        delta_transform=deltaTransform(self.configLoader,self.timedlogger,self.spark,self.dbutils)
        res=delta_transform.get_df_source_from_temp_table()
        print(res)
        self.assertTrue(res==0)
    
    def test_add_metadata_cols(self):
        tbl_name='pst_i18n_regions_metastore'
        self.configLoader.read_table_config(tbl_name)
        delta_transform=deltaTransform(self.configLoader,self.timedlogger,self.spark,self.dbutils)
        res=delta_transform.add_metadata_cols()
        delta_transform.df_source.printSchema()
        self.assertTrue(res==0)
    
    def test_join_to_fk_table(self):
        tbl_name='pst_i18n_regions_metastore'
        self.configLoader.read_table_config(tbl_name)
        delta_transform=deltaTransform(self.configLoader,self.timedlogger,self.spark,self.dbutils)
        delta_transform.read_mapping_csv(tbl_name)
        res=delta_transform.join_to_fk_table()
        print('===========================================test_join_to_fk_table===========================================================')
        delta_transform.df_source.show()
        self.assertTrue(res==0)

    def test_join_to_fk_table_scd2(self):
        tbl_name='pst_i18n_regions_metastore_scd2'
        self.configLoader.read_table_config(tbl_name)
        delta_transform=deltaTransform(self.configLoader,self.timedlogger,self.spark,self.dbutils)
        delta_transform.read_mapping_csv(tbl_name)
        res=delta_transform.join_to_fk_table()
        self.assertTrue(res==0)

    def test_df_source_pk(self):
        tbl_name='stg_i18n_regions_df'
        self.configLoader.read_table_config(tbl_name)
        read_schema=CustomSchema.lnd_ytb_i18nRegions_schema()
        df=jsonExtract(self.dbutils,self.spark,self.configLoader,self.timedlogger).create_df_from_landing(self.read_schema)
        delta_transform=deltaTransform(self.configLoader,self.timedlogger,self.spark,self.dbutils,read_schema=None,df=df)
        res=delta_transform.get_source_pk()
        delta_transform.df_source.show()
        self.assertTrue(res==0)
        
    def tearDown(self):
        pass

if __name__ == "__main__":
    import xmlrunner
    def run_tests():
        test_classes_to_run=[delta_transform_unittest]
        loader=unittest.TestLoader()
        suites_list=[]
        for test_class in test_classes_to_run:
            suite=loader.loadTestsFromTestCase(test_class)
            suites_list.append(suite)
        all_suite=unittest.TestSuite(suites_list)
        runner=xmlrunner.XMLTestRunner(output=os.getcwd()+'/test/')
        runner.run(all_suite)

    run_tests()