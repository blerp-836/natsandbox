import os, uuid, sys,logging
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__
from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError

from pathlib import Path
import configparser

TOP_DIR = Path(__file__).resolve().parent.parent
config = configparser.ConfigParser()
config.read(str(TOP_DIR.joinpath('utilities','config','credentials.ini')))

logger = logging.getLogger(__name__)

class BlobStorage():
    def __init__(self):
        self.connect_str = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        # Create the BlobServiceClient object which will be used to create a container client
        self.blob_service_client = BlobServiceClient.from_connection_string(self.connect_str)
        

    def create_container(self,container_name):
        self.blob_service_client.create_container(container_name)
    
    def delete_container(self,container_name):
        self.container_client=self.blob_service_client.get_container_client(container_name)
        self.container_client.delete_container()

    def get_container_client(self,container_name):
        try:
            self.create_container(container_name)
            print ('container created. getting the container client')
                   
        except ResourceExistsError as e:
            print (e)
            print('container already exists. getting the container client')
            pass
        
        except:
            raise IOError

        self.container_client=self.blob_service_client.get_container_client(container_name)

        print(self.container_client)

    def upload(self, source, dest,skipLevel=False):
        '''
        Upload a file or directory to a path inside the container
        '''
        self.get_container_client(dest)

        if (os.path.isdir(source)):
            self.upload_dir(source, dest,skipLevel)
        else:
            self.upload_file(source, dest)


    def upload_file(self, source, dest):
        '''
        Upload a single file to a path inside the container
        '''
        print(f'Uploading {source} to {dest}')

        with open(source, 'rb') as data:
            try:
                self.container_client.upload_blob(name=dest, data=data)
            except ResourceExistsError as e:
                print(e)
                try:
                    print ('getting blob client')
                    self.blob_client=self.container_client.get_blob_client(dest)
                    print('deleting blob')
                    self.blob_client.delete_blob()
                    print('uploading new blob')
                    self.container_client.upload_blob(name=dest, data=data)
                except:
                    raise IOError
                
            except Exception as e:
                raise IOError

    def upload_dir(self, source, dest,skipLevel):
        '''
        Upload a directory to a path inside the container
        '''
        dest='' if skipLevel else dest
        prefix = '' if dest == '' else dest + '/'
        prefix += os.path.basename(source) + '/'
        for root, dirs, files in os.walk(source):
            for name in files:
                dir_part = os.path.relpath(root, source)
                dir_part = '' if dir_part == '.' else dir_part + '/'
                file_path = os.path.join(root, name)
                blob_path = prefix + dir_part + name
                self.upload_file(file_path, blob_path)

if __name__=='__main__':
    blob=BlobStorage()
    #blob.get_container_client('deploy')
    try:
        #print('container already exists')
        print('deleting container')
        blob.delete_container('deploy')
        import time
        time.sleep(120)

        
    except ResourceNotFoundError as e:
        print('container does not not exists. pass')
        try:
            #sleep(120)
            blob.upload('/home/natmsdnadmin/develop/sandbox/adls/jobs','deploy',True)
        
        except:
            raise IOError
    





            
        
