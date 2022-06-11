# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ytb_analytics_int;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table IF EXISTS ytb_analytics_int.int_i18n_regions;

# COMMAND ----------

dbutils.fs.rm('dbfs:/mnt/natmsdnadlsdatabricks/delta/integration/i18nRegions/incoming',True)

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table IF NOT EXISTS ytb_analytics_int.int_i18n_regions(
# MAGIC id string,
# MAGIC iso_country_code string,
# MAGIC region_name string,
# MAGIC extractDate date,
# MAGIC load_date_time timestamp,
# MAGIC countryCode string,
# MAGIC load_date date) USING DELTA 
# MAGIC PARTITIONED BY (countryCode,load_date_time)
# MAGIC LOCATION 'dbfs:/mnt/natmsdnadlsdatabricks/delta/integration/i18nRegions/incoming'

# COMMAND ----------

dbutils.fs.rm('dbfs:/mnt/natmsdnadlsdatabricks/delta/integration/i18nRegions_scd2/incoming',True)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table IF EXISTS ytb_analytics_int.int_i18n_regions_scd2_test;

# COMMAND ----------

# MAGIC %sql
# MAGIC Create table IF NOT EXISTS ytb_analytics_int.int_i18n_regions_scd2_test(
# MAGIC id string,
# MAGIC iso_country_code string,
# MAGIC region_name string,
# MAGIC extractDate date,
# MAGIC load_date_time timestamp,
# MAGIC eff_start_dt timestamp,
# MAGIC eff_end_dt timestamp,
# MAGIC countryCode string,
# MAGIC load_date date) USING DELTA 
# MAGIC PARTITIONED BY (countryCode,load_date_time)
# MAGIC LOCATION 'dbfs:/mnt/natmsdnadlsdatabricks/delta/integration/i18nRegions_scd2/incoming'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS ytb_analytics_stg;

# COMMAND ----------

dbutils.fs.rm('dbfs:/mnt/natmsdnadlsdatabricks/delta/staging/i18nRegions/incoming',True)

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists ytb_analytics_stg.stg_i18n_regions

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ytb_analytics_stg.stg_i18n_regions(
# MAGIC  etag string,
# MAGIC  `items` array<struct<etag:string,id:string,kind:string,snippet:struct<gl:string,name:string>>>,
# MAGIC  kind string,
# MAGIC  load_date_time timestamp,
# MAGIC  countryCode string,
# MAGIC  load_date date)
# MAGIC  USING DELTA
# MAGIC  PARTITIONED BY (countryCode,load_date_time)
# MAGIC  LOCATION 'dbfs:/mnt/natmsdnadlsdatabricks/delta/staging/i18nRegions/incoming'

# COMMAND ----------

dbutils.fs.rm('dbfs:/mnt/natmsdnadlsdatabricks/delta/presentation/i18nRegions/incoming',True)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists ytb_analytics_pst

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists ytb_analytics_pst.pst_i18n_regions

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ytb_analytics_pst.pst_i18n_regions(
# MAGIC   src_country_pk int,
# MAGIC   dim_regions_id string,
# MAGIC   dim_iso_country_code string,
# MAGIC   dim_region_name string,
# MAGIC   dim_extract_date date,
# MAGIC   countryCode string,
# MAGIC   modified_ts timestamp,
# MAGIC   load_date date)
# MAGIC  USING DELTA
# MAGIC  PARTITIONED BY (countryCode,modified_ts)
# MAGIC  LOCATION 'dbfs:/mnt/natmsdnadlsdatabricks/delta/presentation/i18nRegions/incoming'

# COMMAND ----------

dbutils.fs.rm('dbfs:/mnt/natmsdnadlsdatabricks/delta/integration/videoCategories/incoming',True)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists ytb_analytics_int

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists ytb_analytics_int.int_video_cat

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ytb_analytics_int.int_video_cat(
# MAGIC id string,
# MAGIC assignable boolean,
# MAGIC title string,
# MAGIC extractDate date,
# MAGIC load_date_time timestamp,
# MAGIC countryCode string,
# MAGIC load_date date
# MAGIC )USING DELTA 
# MAGIC PARTITIONED BY (countryCode,load_date_time)
# MAGIC LOCATION 'dbfs:/mnt/natmsdnadlsdatabricks/delta/integration/videoCategories/incoming'

# COMMAND ----------

dbutils.fs.rm('dbfs:/mnt/natmsdnadlsdatabricks/delta/staging/videoCategories/incoming',True)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists ytb_analytics_stg;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists ytb_analytics_stg.stg_video_cat

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ytb_analytics_stg.stg_video_cat(
# MAGIC etag string,
# MAGIC `items` array<struct<etag:string,id:string,kind:string,snippet:struct<assignable:boolean,channelId:string,title:string>>>,
# MAGIC kind string,
# MAGIC load_date_time timestamp,
# MAGIC countryCode string,
# MAGIC load_date date) USING DELTA 
# MAGIC PARTITIONED BY (countryCode,load_date_time)
# MAGIC LOCATION 'dbfs:/mnt/natmsdnadlsdatabricks/delta/staging/videoCategories/incoming'

# COMMAND ----------

dbutils.fs.rm('dbfs:/mnt/natmsdnadlsdatabricks/delta/presentation/videoCategories/incoming',True)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists ytb_analytics_pst;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists ytb_analytics_pst.pst_video_cat;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ytb_analytics_pst.pst_video_cat(
# MAGIC src_video_categories_pk int,
# MAGIC dim_video_categories_id string,
# MAGIC dim_assignable boolean,
# MAGIC dim_title string,
# MAGIC dim_extract_date date,
# MAGIC modified_ts timestamp,
# MAGIC countryCode string,
# MAGIC load_date date
# MAGIC )USING DELTA 
# MAGIC PARTITIONED BY (countryCode,modified_ts)
# MAGIC LOCATION 'dbfs:/mnt/natmsdnadlsdatabricks/delta/presentation/videoCategories/incoming'

# COMMAND ----------

dbutils.fs.rm('dbfs:/mnt/natmsdnadlsdatabricks/delta/staging/video/raw/incoming',True)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists ytb_analytics_stg;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists ytb_analytics_stg.stg_video_raw;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ytb_analytics_stg.stg_video_raw(
# MAGIC SequenceNumber bigint ,
# MAGIC Offset string ,
# MAGIC EnqueuedTimeUtc string ,
# MAGIC SystemProperties map<string,struct<member0:bigint,member1:double,member2:string,member3:binary>> ,
# MAGIC Properties map<string,struct<member0:bigint,member1:double,member2:string,member3:binary>> ,
# MAGIC Body struct<etag:string,items:array<struct<etag:string,id:string,kind:string,snippet:struct<categoryId:string,channelId:string,channelTitle:string,defaultAudioLanguage:string,defaultLanguage:string,description:string,liveBroadcastContent:string,localized:struct<description:string,title:string>,publishedAt:timestamp,tags:array<string>,thumbnails:struct<default:struct<height:double,url:string,width:double>,high:struct<height:double,url:string,width:double>,maxres:struct<height:double,url:string,width:double>,medium:struct<height:double,url:string,width:double>,standard:struct<height:double,url:string,width:double>>,title:string>,statistics:struct<commentCount:string,dislikeCount:string,favoriteCount:string,likeCount:string,viewCount:string>>>,kind:string,nextPageToken:string,pageInfo:struct<resultsPerPage:double,totalResults:double>> ,
# MAGIC load_date_time timestamp ,
# MAGIC countryCode string ,
# MAGIC load_date date) USING DELTA
# MAGIC PARTITIONED BY (countryCode, load_date)
# MAGIC LOCATION 'dbfs:/mnt/natmsdnadlsdatabricks/delta/staging/video/raw/incoming'

# COMMAND ----------

dbutils.fs.rm('dbfs:/mnt/natmsdnadlsdatabricks/delta/staging/video/processed/incoming',True)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists ytb_analytics_stg;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists ytb_analytics_stg.stg_video_processed;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ytb_analytics_stg.stg_video_processed(
# MAGIC etag string ,
# MAGIC `items` array<struct<etag:string,id:string,kind:string,snippet:struct<categoryId:string,channelId:string,channelTitle:string,defaultAudioLanguage:string,defaultLanguage:string,description:string,liveBroadcastContent:string,localized:struct<description:string,title:string>,publishedAt:timestamp,tags:array<string>,thumbnails:struct<default:struct<height:double,url:string,width:double>,high:struct<height:double,url:string,width:double>,maxres:struct<height:double,url:string,width:double>,medium:struct<height:double,url:string,width:double>,standard:struct<height:double,url:string,width:double>>,title:string>,statistics:struct<commentCount:string,dislikeCount:string,favoriteCount:string,likeCount:string,viewCount:string>>> ,
# MAGIC kind string ,
# MAGIC nextPageToken string ,
# MAGIC pageInfo struct<resultsPerPage:double,totalResults:double> ,
# MAGIC load_date_time timestamp ,
# MAGIC countryCode string ,
# MAGIC load_date date) USING DELTA
# MAGIC PARTITIONED BY (countryCode,load_date_time)
# MAGIC LOCATION 'dbfs:/mnt/natmsdnadlsdatabricks/delta/staging/video/processed/incoming'

# COMMAND ----------

dbutils.fs.rm('dbfs:/mnt/natmsdnadlsdatabricks/delta/integration/video/incoming',True)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists ytb_analytics_int;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists ytb_analytics_int.int_video;

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists ytb_analytics_int.int_video(
# MAGIC id string ,
# MAGIC title string ,
# MAGIC channelId string ,
# MAGIC channelTitle string ,
# MAGIC categoryId string ,
# MAGIC tags array<string> ,
# MAGIC commentCount int ,
# MAGIC dislikeCount int ,
# MAGIC favoriteCount int ,
# MAGIC likeCount int ,
# MAGIC viewCount int ,
# MAGIC ratingDisabled boolean ,
# MAGIC commentDisabled boolean ,
# MAGIC publishedAt timestamp ,
# MAGIC extractDate date ,
# MAGIC load_date_time timestamp ,
# MAGIC countryCode string ,
# MAGIC load_date date) USING DELTA
# MAGIC PARTITIONED BY (countryCode,load_date_time)
# MAGIC LOCATION 'dbfs:/mnt/natmsdnadlsdatabricks/delta/integration/video/incoming'

# COMMAND ----------

dbutils.fs.rm('dbfs:/mnt/natmsdnadlsdatabricks/delta/presentation/video/incoming',True)

# COMMAND ----------

# MAGIC %sql
# MAGIC create database if not exists ytb_analytics_pst;

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists ytb_analytics_pst.pst_video;

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create table if not exists ytb_analytics_pst.pst_video(
# MAGIC fct_video_id string,
# MAGIC fct_title string,
# MAGIC fct_channel_id string,
# MAGIC fct_channel_title string,
# MAGIC video_categories_fk int,
# MAGIC fct_tags array<string>,
# MAGIC fct_comment_count int,
# MAGIC fct_dislike_count int,
# MAGIC fct_favorite_count int,
# MAGIC fct_like_count int,
# MAGIC fct_view_count int,
# MAGIC fct_rating_disabled boolean,
# MAGIC fct_comment_disabled boolean,
# MAGIC fct_published_at timestamp,
# MAGIC fct_extract_date date,
# MAGIC modified_ts timestamp,
# MAGIC countryCode string,
# MAGIC load_date date) using delta
# MAGIC 
# MAGIC partitioned by (countryCode, modified_ts)
# MAGIC location 'dbfs:/mnt/natmsdnadlsdatabricks/delta/presentation/video/incoming'
