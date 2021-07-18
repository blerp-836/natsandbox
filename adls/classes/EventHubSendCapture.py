import requests, sys, time, os, argparse,datetime,json
from pathlib import Path
import subprocess
import asyncio

from adls.classes.ConfigLoader import ConfigLoader
from utilities import KeyVault,AdlsFs
from azure.eventhub.aio import EventHubProducerClient
from azure.eventhub import EventData

class EventHubSendCapture():
    def __init__(self,dbutils,configLoader,logger):
        self.configLoader=configLoader
        self.dbutils=dbutils
        #use dbutils when migrating to databricks
        #self.api_key=self.dbutils.secrets.get(scope = "dbconnect-akv", key = self.configLoader.config['API']['API_KEY'])
        self.keyVault=KeyVault.KeyVault()
        self.api_key=self.keyVault.main(self.configLoader.config['API']['api_key'])
        self.request_url=eval(self.configLoader.config['API']['request_url'])
        self.country_code=self.configLoader.config['API']['country_code']
        self.next_page_token=eval(self.configLoader.config['API']['page_token'])
        self.adlsFs=AdlsFs.AdlsFs()
        self.logger=logger.logger
        self.event_hub_name=self.configLoader.config['SETTINGS']['event_hub_name']
        self.event_hub_connstr=self.keyVault.main(self.configLoader.config['SETTINGS']['event_hub_connstr'])
        self.event_hub_batch_adds=eval(self.configLoader.config['SETTINGS']['event_hub_batch_num'])
    def api_request(self):
        try:
            page_token=self.next_page_token
            country_code=self.country_code
            # Builds the URL and requests the JSON from it
            request_url=self.request_url.format(page_token=page_token,country_code=country_code,api_key=self.api_key)
            
            request = requests.get(request_url)
            if request.status_code == 429:
                print("Temp-Banned due to excess requests, please wait and continue later")
                self.logger.error('request banned due to excess request. try again later')
                raise IOError
                sys.exit()
            if request.status_code==400:
                print('request banned due to invalid criteria')
                self.logger.error('request banned due to invalid criteria')
                raise IOError
                sys.exit()
        except:
            self.logger.error('api request failed')
            raise IOError
        
        self.logger.info('api request is successfully returned. creating json........')
        
        return request.json()       

    async def run(self):
        # Create a producer client to send messages to the event hub.
        # Specify a connection string to your event hubs namespace and
        # the event hub name.
        producer = EventHubProducerClient.from_connection_string(conn_str=self.event_hub_connstr, eventhub_name=self.event_hub_name)
        async with producer:
            # Create a batch.
            event_data_batch = await producer.create_batch()
            # Add events to the batch.
            for i in range(5):
                reading=json.dumps(self.api_request())
                event_data_batch.add(EventData(reading))
                self.logger.info('add batch #{0}'.format(i))
            # Send the batch of events to the event hub.
            await producer.send_batch(event_data_batch)
            self.logger.info('sending data to event hub')
            

    def run_until_complete(self):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.run())
        self.logger.info('RUN COMPLETED')
    

            
        