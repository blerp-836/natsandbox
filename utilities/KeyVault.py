import sys
import logging
import configparser
import time
from pathlib import Path

from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient
from azure.core.exceptions import (ClientAuthenticationError,
                                ServiceRequestError,
                                ResourceNotFoundError,
                                )

logger = logging.getLogger(__name__)

TOP_DIR = Path(__file__).resolve().parent.parent

config = configparser.ConfigParser()
config.read(str(TOP_DIR.joinpath('utilities','config','credentials.ini')))
print (str(TOP_DIR.joinpath('utilities','config','credentials.ini')))

from azure.identity import ClientSecretCredential
from azure.keyvault.secrets import SecretClient

class KeyVault():
    def __init__(self):
        self.tenant_id = config['AzureAuth']['tenant_id']                          
        self.vault_url = config['AzureAuth']['vault_url']   
        self.client_id = config['AzureAuth']['client_id']                          
        self.cert_path = config['AzureAuth']['client_secret']                       

    def main(self,secret_name):
        #AUTHENTICATION TO Azure Active Directory USING CLIENT ID AND CLIENT CERTIFICATE (GET Azure Active Directory TOKEN)
        token = ClientSecretCredential(tenant_id=self.tenant_id, client_id=self.client_id, client_secret=self.cert_path)
        #AUTHENTICATION TO KEY VAULT PRESENTING Azure Active Directory TOKEN
        client = SecretClient(vault_url=self.vault_url, credential=token)
        #CALL TO KEY VAULT TO GET SECRET
        #ENTER NAME OF A SECRET STORED IN KEY VAULT
        for i in range (4):
            try:
                secret = client.get_secret(secret_name)
                
                return secret.value
                
            except ServiceRequestError:
                if i < 3:
                    wait = 5 * (i +1)
                    logger.warning('Unable to reach the KeyVault. retrying in {} seconds'.format(wait))
                    time.sleep(wait)
                    continue
                else:
                    logger.error('Could not connect to the Azure Keyvault after {} tries. Check that the server is available'.format('4'))
                    raise
                    sys.exit(1)

            except ClientAuthenticationError:
                logger.error('Credentials provided are invalid. Authentication failed.')
                raise
                sys.exit(1)

            except ResourceNotFoundError:
                logger.error('The requested secret: {} does not exist'.format(secret_name))
                raise
                sys.exit(1)


if __name__=="__main__":
    keyVault=KeyVault()
    print(keyVault.main('AZ-SQL-PASS'))
        



