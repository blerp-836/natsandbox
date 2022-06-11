# Databricks notebook source
directory_id=dbutils.secrets.get(scope="databricks-akv",key="adls-app-registration-directory-id")
application_id=dbutils.secrets.get(scope="databricks-akv",key="adls-app-registration-app-id")
application_secret=dbutils.secrets.get(scope="databricks-akv",key="adls-app-registration-secret")

# COMMAND ----------

def mountAdls(container_name,storage_account_name):
    try:
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
    except:
        pass

    try:
        configs = {"fs.azure.account.auth.type": "OAuth",
              "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
              "fs.azure.account.oauth2.client.id": application_id,
              "fs.azure.account.oauth2.client.secret": application_secret,
              "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/{0}/oauth2/token".format(directory_id)}

        # Optionally, you can add <directory-name> to the source URI of your mount point.
        dbutils.fs.mount(
          source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
          mount_point = f"/mnt/{storage_account_name}/{container_name}",
          extra_configs = configs)
        print('===listing mount points content===')
        print(dbutils.fs.ls(f"/mnt/{storage_account_name}/{container_name}"))
    except:
        raise IOError
    
    

# COMMAND ----------

mountAdls('landing', 'natmsdnadlsdatabricks')
mountAdls('staging', 'natmsdnadlsdatabricks')
mountAdls('integration', 'natmsdnadlsdatabricks')
mountAdls('delta', 'natmsdnadlsdatabricks')



# COMMAND ----------

dbutils.fs.ls('dbfs:/mnt/natmsdnadlsdatabricks/delta/staging/video/raw/incoming/')
