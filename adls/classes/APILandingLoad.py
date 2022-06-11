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

class APILandingLoad():
    def __init__(self,dbutils,spark,configLoader,logger,mode):
        self.mode=mode
        self.configLoader=configLoader
        self.dbutils=dbutils
        #use dbutils when migrating to databricks
        self.api_key=self.dbutils.secrets.get(scope = "databricks-akv", key = self.configLoader.config['API']['API_KEY'])
        self.keyVault=KeyVault.KeyVault()
        #self.api_key=self.keyVault.main(self.configLoader.config['API']['api_key'])
        self.adls_key=self.dbutils.secrets.get(scope='databricks-akv',key=self.configLoader.config['SETTINGS']['adls_key'])
        self.spark=spark
        self.request_url=eval(self.configLoader.config['API']['request_url'])
        self.country_code=self.configLoader.config['API']['country_code']
        self.next_page_token=eval(self.configLoader.config['API']['page_token'])
        self.incoming_file=configLoader.config['SETTINGS']['landing_file']
        self.adlsFs=AdlsFs.AdlsFs()
        self.logger=logger.logger
        self.__getlocal_path()
        
    def __getlocal_path(self):
        if self.mode.lower()=='local':
            TOP_DIR = Path(__file__).resolve().parent.parent.parent
            self.local_path=str(TOP_DIR)+self.configLoader.config['SETTINGS']['temp_path']
        else:
            #dbfs path where temp file is loaded
            self.local_path=self.configLoader.config['SETTINGS']['temp_path']
            pass

    def api_request(self):
        try:
            page_token=self.next_page_token
            country_code=self.country_code
            # Builds the URL and requests the JSON from it
            request_url=self.request_url.format(page_token=page_token,country_code=country_code,api_key=self.api_key)
            
            request = requests.get(request_url)
            if request.status_code == 429:
                print("Temp-Banned due to excess requests, please wait and continue later")
                self.logger.error('temporarily banned due to excess request. try again later')
                raise IOError
                sys.exit()
            if request.status_code==400:
                print('request banned due to invalid criteria')
                self.logger.error('request banned due to invalid criteria. check the request url')
                raise IOError
                sys.exit()
        except:
            self.logger.error('api request failed')
            raise IOError
        
        self.logger.info('api request is successfully returned. creating json........')
        return request.json()       

    def __create_temp_dir(self):
        try:
            if not os.path.exists(self.local_path):
                os.makedirs(self.local_path)
        except:
            self.logger.error('failed to create local temp dir')
            raise IOError
    
    def __create_log_dir(self):
        try:
            if not os.path.exists(str(TOP_DIR)+self.configLoader.config['SETTINGS']['temp_path']):
                os.makedirs(str(TOP_DIR)+self.configLoader.config['SETTINGS']['temp_path'])
        except:
            self.logger.error('failed to create log dir')
            raise IOError('failed to create log directory')

    def __delete_temp_file(self):
        for filename in os.listdir(self.local_path):
            file_path = os.path.join(self.local_path, filename)
            try:
                if os.path.isfile(file_path) or os.path.islink(file_path):
                    os.unlink(file_path)
                elif os.path.isdir(file_path):
                    shutil.rmtree(file_path)
                self.logger.info('files are deleted from edge node')
            except Exception as e:
                self.logger.error('failed to delete files from edge node')
                raise IOError('failed to delete files frome edge node')
                print('Failed to delete %s. Reason: %s' % (file_path, e))


    def __create_landing_file(self):
        try:

            print(self.local_path+self.incoming_file)
            with open (self.local_path+self.incoming_file,'w') as json_file:
                json.dump(self.api_request(),json_file)
            self.logger.info('json file containing api request result has been created')
        except:
            self.logger.error('failed to create json file containing api request result')
            raise IOError
    
    def __load_to_landing(self):
        try:
            NOW = datetime.datetime.now().strftime("%m%d%Y_%H%M%S")
            self.landing_file=self.country_code+'_'+self.incoming_file[:-5]+'_'+NOW+'.json'
            self.adlsFs.upload_file_to_directory_bulk(self.configLoader.config['SETTINGS']['landing'],'incoming',self.local_path,self.incoming_file,self.landing_file)
            self.logger.info('files loaded to landing')
            storage_account_name=self.configLoader.config['SETTINGS']['storage_account_name']
            container_name=self.configLoader.config['SETTINGS']['landing']            
            #path=f'dbfs:/mnt/{storage_account_name}/{container_name}/incoming/{self.landing_file}'
            #self.logger.info(path)
            #self.configLoader.write_to_job_config(path)
        except:
            self.logger.error('failed to load to landing')
            raise IOError
    


    
    def move_to_archive(self):
        path=f'{self.configLoader.config["SETTINGS"]["landing"]}/incoming/'
        path=path.split('/',1)
        if path[0]=='':
            path=path[1].split('/',1)
        adlspaths=self.adlsFs.list_directory_contents(path[0],path[1])
        for path in adlspaths:
            incoming_path=path
            archive_path=path.replace('/incoming/','/archive/')
            command=f"az storage fs file move --new-path landing/{archive_path} --path {incoming_path} --file-system landing --account-name natmsdnadlsdatabricks\
                 --account-key {self.adls_key} --auth-mode key"
            print(command.split())
            try:
                subprocess.run(command.split(),check=True)
            except:
                raise IOError('failed to move file to archive folder')
    
    
    def landing_load(self):
        #self.__create_log_dir()
        self.__create_temp_dir()
        self.__create_landing_file()
        self.__load_to_landing()
        self.__delete_temp_file()

    
            

            

        


        
    