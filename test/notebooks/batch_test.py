# Databricks notebook source
from pyspark.sql.functions import col

# COMMAND ----------

api='i18nRegions'
path=f'dbfs:/mnt/natmsdnadlsdatabricks/integration/{api}/incoming'

# COMMAND ----------

spark.read.parquet(path).orderBy(col('load_date_time').desc()).show(20,False)

# COMMAND ----------

col_string=list()
for i in spark.read.parquet(path).dtypes:
    col_string.append(i[0]+' '+i[1])
col_string=",".join(col_string)
print(col_string)

# COMMAND ----------

spark.read.parquet(path).select('load_date_time').distinct().show()

# COMMAND ----------

api='videoCategories'
path=f'dbfs:/mnt/natmsdnadlsdatabricks/staging/{api}/incoming'

# COMMAND ----------

spark.read.parquet(path).show()

# COMMAND ----------

col_string=list()
for i in spark.read.parquet(path).dtypes:
    col_string.append(i[0]+' '+i[1])
col_string=",".join(col_string)
print(col_string)

# COMMAND ----------

spark.read.parquet(path).orderBy(col('load_date_time').desc()).show()

# COMMAND ----------

api='video'
path=f'dbfs:/mnt/natmsdnadlsdatabricks/integration/{api}/incoming'

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

spark.read.parquet(path).show()

# COMMAND ----------

col_string=list()
for i in spark.read.parquet(path).dtypes:
    col_string.append(i[0]+' '+i[1])
for j in col_string:
    print(j,',')
col_string=",".join(col_string)



# COMMAND ----------

spark.read.parquet(path).select('load_date_time').distinct().show()
