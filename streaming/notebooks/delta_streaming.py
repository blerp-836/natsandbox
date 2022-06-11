# Databricks notebook source
namespaceName='natmsdneventhubnamespace'
eventHubName = "eventhub02s"
sasKeyName = dbutils.secrets.get('databricks-akv',"natmsdneventhub02s-sas-policy")
sasKey = dbutils.secrets.get('databricks-akv',"natmsdneventhub02s-sas-key")
conf = {}
connectionString=dbutils.secrets.get('databricks-akv','natmsdneventhub-connstr-streaming')
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

#create staging faw dataframe
stg_raw_streaming = temp.withColumn("body", from_json(temp.body,lnd_body_schema, jsonOptions))\
            .withColumn('load_date_time',lit(datetime.datetime.now())).withColumn('load_date',lit(datetime.date.today()))\
            .withColumn('countryCode',lit('CA'))\
            .selectExpr('sequenceNumber as SequenceNumber','offset as Offset','cast(enqueuedTime as string) as EnqueuedTimeUtc','body as Body','load_date_time','load_date','countryCode')

# COMMAND ----------

checkpointLoc='dbfs:/mnt/natmsdnadlsdatabricks/delta/streaming/ytb_video_checkpoint/'

# COMMAND ----------

staging_raw='dbfs:/mnt/natmsdnadlsdatabricks/delta/staging/video/raw/incoming'

# COMMAND ----------

stg_raw_streaming.writeStream.queryName('stg_raw_streaming_query').format("delta").partitionBy('countryCode','load_date').\
  option("checkpointLocation", checkpointLoc)\
    .option('maxFilesPerTrigger',5)\
  .outputMode("append")\
  .start(staging_raw)

# COMMAND ----------


