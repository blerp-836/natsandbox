from configparser import ConfigParser, ExtendedInterpolation
import ast

class ConfigLoader():
    def __init__(self,path,job_name):
        self.configpath=path
        self.config=None
        self.job_name=job_name
    
    def init_config(self):  
        self.config=ConfigParser()
    
    def read_default_config(self):
        self.init_config()
        default_config=self.configpath+'/config/default.ini'
        #print(default_config)
        self.config.read(default_config)
        #print(self.config.sections())
        return self.config

    def read_job_config(self):
        env_config=self.configpath+'/jobs/{0}/config/env.ini'.format(self.job_name)
        #print (env_config)
        self.config.read(env_config)
        #print (self.config.sections())
        self.job_config=self.configpath+'/jobs/{0}/config/'.format(self.job_name)+self.config['default']['ENV'].lower()+'.ini'
        self.config.read(self.job_config)
        print(self.config.sections())
        return self.config
    
    def write_to_job_config(self,path):
        self.config['TEMP']['temp']=path
        with open (self.job_config,'w') as configfile:
            self.config.write(configfile)


