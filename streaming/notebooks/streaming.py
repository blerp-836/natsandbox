# Databricks notebook source
namespaceName='natmsdneventhubnamespace'
eventHubName = "eventhub02s"
sasKeyName = dbutils.secrets.get('dbconnect-akv',"natmsdneventhub02s-sas-policy")
sasKey = dbutils.secrets.get('dbconnect-akv',"natmsdneventhub02s-sas-key")
conf = {}
connectionString=dbutils.secrets.get('dbconect-akv','natmsdneventhub-connstr-streaming')
conf['eventhubs.connectionString'] = sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(connectionString)


# COMMAND ----------

countryCode='CA'

# COMMAND ----------

temp = spark.readStream.format("org.apache.spark.sql.eventhubs.EventHubsSourceProvider").options(**conf).load()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

temp = temp.withColumn("body", col("body").cast("string"))

# COMMAND ----------

jsonOptions = {"dateFormat" : "yyyy-MM-dd HH:mm:ss.SSS"}

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType,TimestampType,DoubleType,BooleanType,DateType

# COMMAND ----------

     lnd_body_schema=StructType([StructField('etag',StringType(),True),StructField('items',ArrayType(StructType([StructField('etag',StringType(),True),StructField('id',StringType(),True),StructField('kind',StringType(),True),StructField('snippet',StructType([StructField('categoryId',StringType(),True),StructField('channelId',StringType(),True),StructField('channelTitle',StringType(),True),StructField('defaultAudioLanguage',StringType(),True),StructField('defaultLanguage',StringType(),True),StructField('description',StringType(),True),StructField('liveBroadcastContent',StringType(),True),StructField('localized',StructType([StructField('description',StringType(),True),StructField('title',StringType(),True)]),True),StructField('publishedAt',TimestampType(),True),StructField('tags',ArrayType(StringType(),True),True),StructField('thumbnails',StructType([StructField('default',StructType([StructField('height',DoubleType(),True),StructField('url',StringType(),True),StructField('width',DoubleType(),True)]),True),StructField('high',StructType([StructField('height',DoubleType(),True),StructField('url',StringType(),True),StructField('width',DoubleType(),True)]),True),StructField('maxres',StructType([StructField('height',DoubleType(),True),StructField('url',StringType(),True),StructField('width',DoubleType(),True)]),True),StructField('medium',StructType([StructField('height',DoubleType(),True),StructField('url',StringType(),True),StructField('width',DoubleType(),True)]),True),StructField('standard',StructType([StructField('height',DoubleType(),True),StructField('url',StringType(),True),StructField('width',DoubleType(),True)]),True)]),True),StructField('title',StringType(),True)]),True),StructField('statistics',StructType([StructField('commentCount',StringType(),True),StructField('dislikeCount',StringType(),True),StructField('favoriteCount',StringType(),True),StructField('likeCount',StringType(),True),StructField('viewCount',StringType(),True)]),True)]),True),True),StructField('kind',StringType(),True),StructField('nextPageToken',StringType(),True),StructField('pageInfo',StructType([StructField('resultsPerPage',DoubleType(),True),StructField('totalResults',DoubleType(),True)]),True)])

# COMMAND ----------

from pyspark.sql.functions import from_json, lit
import datetime

# COMMAND ----------

temp = temp.withColumn("body", from_json(temp.body,lnd_body_schema, jsonOptions))

# COMMAND ----------

for c in temp.schema['body'].dataType:
    temp = temp.withColumn(c.name, col("body." + c.name))

# COMMAND ----------

temp=temp.drop('body')

# COMMAND ----------

from pyspark.sql.functions import explode_outer

# COMMAND ----------

temp=temp.select(explode_outer(temp.items)).withColumnRenamed('col','items')

# COMMAND ----------

for c in temp.schema['items'].dataType:
  temp= temp.withColumn(c.name, col("items." + c.name))


# COMMAND ----------

temp=temp.drop('items')

# COMMAND ----------

for c in temp.schema['snippet'].dataType:
  temp= temp.withColumn(c.name, col("snippet." + c.name))

# COMMAND ----------

for c in temp.schema['statistics'].dataType:
  temp= temp.withColumn(c.name, col("statistics." + c.name))

# COMMAND ----------

temp=temp.drop('snippet').drop('statistics').withColumn('countryCode',lit(countryCode)).withColumn('load_date_time',lit(datetime.datetime.now()))

# COMMAND ----------

temp.createOrReplaceTempView('tempview')

# COMMAND ----------

temp=spark.sql('select countryCode,id, title,channelId,channelTitle,categoryId,tags,cast(commentCount as int), cast(dislikeCount as int),cast(favoriteCount as int),cast(likeCount as int),cast(viewCount as int),publishedAt from tempview')

# COMMAND ----------

streamingdf=temp.groupBy('categoryId','id','title').max('likeCount')


# COMMAND ----------

streamingdf.writeStream.queryName('streamingquery').format('console').outputMode('complete').start()
