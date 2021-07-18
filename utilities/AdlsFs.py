import os, uuid, sys,logging
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings
from pathlib import Path
import configparser
from azure.identity import ClientSecretCredential
TOP_DIR = Path(__file__).resolve().parent.parent
config = configparser.ConfigParser()
config.read(str(TOP_DIR.joinpath('utilities','config','credentials.ini')))
#print (str(TOP_DIR.joinpath('utilities','config','credentials.ini')))

service_client=None
file_system_client=None
class AdlsFs():
    def __init__(self):
        self.tenant_id = config['AzureAuth']['tenant_id']                           
        self.client_id = config['AzureAuth']['client_id']                          
        self.cert_path = config['AzureAuth']['client_secret'] 
        self.initialize_storage_account_ad('natmsdnadlsdatabricks')

    def initialize_storage_account_ad(self,storage_account_name):
        
        try:  
            global service_client
            credential = ClientSecretCredential(self.tenant_id, self.client_id, self.cert_path)
            service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
                "https", storage_account_name), credential=credential)
        
        except Exception as e:
            print(e)
            raise
    
    def create_file_system(self,my_file_system):
        try:
            global file_system_client
            file_system_client = service_client.create_file_system(file_system=my_file_system)
        
        except Exception as e:
            print(e)
            raise
        
    def delete_directory(self,my_file_system,my_directory):
        try:
            file_system_client = service_client.get_file_system_client(file_system=my_file_system)
            directory_client = file_system_client.get_directory_client(my_directory)
            directory_client.delete_directory()
        except Exception as e:
            print(e)
            raise

    def upload_file_to_directory(self,my_file_system,my_directory,my_file,my_local_path):
        try:
            
            file_system_client = service_client.get_file_system_client(file_system=my_file_system)
            directory_client = file_system_client.get_directory_client(my_directory)
            file_client = directory_client.create_file(my_file)
            local_file = open(mylocal_path+my_file,'r')
            file_contents = local_file.read()
            file_client.append_data(data=file_contents, offset=0, length=len(file_contents))
            file_client.flush_data(len(file_contents))

        except Exception as e:
            print(e)
            raise
    
    def upload_file_to_directory_bulk(self,my_file_system,my_directory,my_path,incoming_file,landing_file):
        try:

            file_system_client = service_client.get_file_system_client(file_system=my_file_system)
            directory_client = file_system_client.get_directory_client(my_directory)
            file_client = directory_client.get_file_client(landing_file)
            local_file = open(my_path+incoming_file,'r')
            file_contents = local_file.read()
            file_client.upload_data(file_contents, overwrite=True)

        except Exception as e:
            print(e)
            raise
    
    def list_directory_contents(self,my_file_system,my_directory):
        try:
            
            file_system_client = service_client.get_file_system_client(file_system=my_file_system)

            paths = file_system_client.get_paths(path=my_directory)
            adlspaths=list()
            for path in paths:
                print(path.name + '\n')
                adlspaths.append(path.name)

        except Exception as e:
            print(e)
            raise
            
        return adlspaths
    
if __name__=="__main__":
    adlsFs=AdlsFs()
    adlspaths=adlsFs.list_directory_contents('landing','video/archive')
    print (adlspaths)
