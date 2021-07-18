# Databricks notebook source
def mountBlob(container_name,storage_account_name):
  try:
    dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")
  except:
    pass
  try:
    dbutils.fs.mount(
      source = f'wasbs://{container_name}@{storage_account_name}.blob.core.windows.net',
      mount_point = f"/mnt/{storage_account_name}/{container_name}",
      extra_configs = {f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net":dbutils.secrets.get(scope = "dbconnect-akv", key = "deployblob123-sas-key")})
  except:
    raise IOerror
  print('===listing mount points content===')
  print(dbutils.fs.ls(f"/mnt/{storage_account_name}/{container_name}"))

# COMMAND ----------

mountBlob('deploy','deployblob123')
mountBlob('logs','deployblob123')


# COMMAND ----------

dbutils.fs.ls('/mnt/deployblob123/')
