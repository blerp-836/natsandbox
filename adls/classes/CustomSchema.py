from pyspark.sql.types import StructType,StructField,StringType,IntegerType,ArrayType,TimestampType,DoubleType,BooleanType,DateType

class CustomSchema():
    @staticmethod
    def lnd_ytb_video_schema():
        spark_schema=StructType([StructField('etag',StringType(),True),StructField('items',ArrayType(StructType([StructField('etag',StringType(),True),StructField('id',StringType(),True),StructField('kind',StringType(),True),StructField('snippet',StructType([StructField('categoryId',StringType(),True),StructField('channelId',StringType(),True),StructField('channelTitle',StringType(),True),StructField('defaultAudioLanguage',StringType(),True),StructField('defaultLanguage',StringType(),True),StructField('description',StringType(),True),StructField('liveBroadcastContent',StringType(),True),StructField('localized',StructType([StructField('description',StringType(),True),StructField('title',StringType(),True)]),True),StructField('publishedAt',TimestampType(),True),StructField('tags',ArrayType(StringType(),True),True),StructField('thumbnails',StructType([StructField('default',StructType([StructField('height',DoubleType(),True),StructField('url',StringType(),True),StructField('width',DoubleType(),True)]),True),StructField('high',StructType([StructField('height',DoubleType(),True),StructField('url',StringType(),True),StructField('width',DoubleType(),True)]),True),StructField('maxres',StructType([StructField('height',DoubleType(),True),StructField('url',StringType(),True),StructField('width',DoubleType(),True)]),True),StructField('medium',StructType([StructField('height',DoubleType(),True),StructField('url',StringType(),True),StructField('width',DoubleType(),True)]),True),StructField('standard',StructType([StructField('height',DoubleType(),True),StructField('url',StringType(),True),StructField('width',DoubleType(),True)]),True)]),True),StructField('title',StringType(),True)]),True),StructField('statistics',StructType([StructField('commentCount',StringType(),True),StructField('dislikeCount',StringType(),True),StructField('favoriteCount',StringType(),True),StructField('likeCount',StringType(),True),StructField('viewCount',StringType(),True)]),True)]),True),True),StructField('kind',StringType(),True),StructField('nextPageToken',StringType(),True),StructField('pageInfo',StructType([StructField('resultsPerPage',DoubleType(),True),StructField('totalResults',DoubleType(),True)]),True)])
        return spark_schema
    
    @staticmethod
    def stg_ytb_video_schema():
        spark_schema=StructType([StructField('etag',StringType(),True),StructField('items',ArrayType(StructType([StructField('etag',StringType(),True),StructField('id',StringType(),True),StructField('kind',StringType(),True),StructField('snippet',StructType([StructField('categoryId',StringType(),True),StructField('channelId',StringType(),True),StructField('channelTitle',StringType(),True),StructField('defaultAudioLanguage',StringType(),True),StructField('defaultLanguage',StringType(),True),StructField('description',StringType(),True),StructField('liveBroadcastContent',StringType(),True),StructField('localized',StructType([StructField('description',StringType(),True),StructField('title',StringType(),True)]),True),StructField('publishedAt',TimestampType(),True),StructField('tags',ArrayType(StringType(),True),True),StructField('thumbnails',StructType([StructField('default',StructType([StructField('height',DoubleType(),True),StructField('url',StringType(),True),StructField('width',DoubleType(),True)]),True),StructField('high',StructType([StructField('height',DoubleType(),True),StructField('url',StringType(),True),StructField('width',DoubleType(),True)]),True),StructField('maxres',StructType([StructField('height',DoubleType(),True),StructField('url',StringType(),True),StructField('width',DoubleType(),True)]),True),StructField('medium',StructType([StructField('height',DoubleType(),True),StructField('url',StringType(),True),StructField('width',DoubleType(),True)]),True),StructField('standard',StructType([StructField('height',DoubleType(),True),StructField('url',StringType(),True),StructField('width',DoubleType(),True)]),True)]),True),StructField('title',StringType(),True)]),True),StructField('statistics',StructType([StructField('commentCount',StringType(),True),StructField('dislikeCount',StringType(),True),StructField('favoriteCount',StringType(),True),StructField('likeCount',StringType(),True),StructField('viewCount',StringType(),True)]),True)]),True),True),StructField('kind',StringType(),True),StructField('nextPageToken',StringType(),True),StructField('pageInfo',StructType([StructField('resultsPerPage',DoubleType(),True),StructField('totalResults',DoubleType(),True)]),True),StructField('countryCode',StringType(),True),StructField('load_date_time',TimestampType(),True),StructField('load_date',DateType(),True)])
        return spark_schema

    
    @staticmethod
    def lnd_ytb_videoCat_schema():
        spark_schema=StructType([StructField('etag',StringType(),True),StructField('items',ArrayType(StructType([StructField('etag',StringType(),True),StructField('id',StringType(),True),StructField('kind',StringType(),True),StructField('snippet',StructType([StructField('assignable',BooleanType(),True),StructField('channelId',StringType(),True),StructField('title',StringType(),True)]),True)]),True),True),StructField('kind',StringType(),True)])
        return spark_schema

    @staticmethod
    def stg_ytb_videoCat_schema():
        spark_schema=StructType([StructField('etag',StringType(),True),StructField('items',ArrayType(StructType([StructField('etag',StringType(),True),StructField('id',StringType(),True),StructField('kind',StringType(),True),StructField('snippet',StructType([StructField('assignable',BooleanType(),True),StructField('channelId',StringType(),True),StructField('title',StringType(),True)]),True)]),True),True),StructField('kind',StringType(),True),StructField('countryCode',StringType(),True),StructField('load_date_time',TimestampType(),True),StructField('load_date',DateType(),True)])
        return spark_schema

    @staticmethod
    def lnd_ytb_i18nRegions_schema():
        spark_schema=StructType([StructField('etag',StringType(),True),StructField('items',ArrayType(StructType([StructField('etag',StringType(),True),StructField('id',StringType(),True),StructField('kind',StringType(),True),StructField('snippet',StructType([StructField('gl',StringType(),True),StructField('name',StringType(),True)]),True)]),True),True),StructField('kind',StringType(),True)])
        return spark_schema
        
    @staticmethod
    def stg_ytb_i18nRegions_schema():
        spark_schema=StructType([StructField('etag',StringType(),True),StructField('items',ArrayType(StructType([StructField('etag',StringType(),True),StructField('id',StringType(),True),StructField('kind',StringType(),True),StructField('snippet',StructType([StructField('gl',StringType(),True),StructField('name',StringType(),True)]),True)]),True),True),StructField('kind',StringType(),True),StructField('countryCode',StringType(),True),StructField('load_date_time',TimestampType(),True),StructField('load_date',DateType(),True)])
        return spark_schema
    
    @staticmethod
    def int_ytb_i18nRegions_schema():
        spark_schema=StructType([StructField('id',StringType(),True),StructField('iso_country_code',StringType(),True),
        StructField('region_name',StringType(),True),StructField('extractDate',StringType(),True),
        StructField('load_date_time',TimestampType(),True),StructField('countryCode',StringType(),True),StructField('load_date',DateType(),True)])
        return spark_schema

