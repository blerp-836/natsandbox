# Databricks notebook source
# MAGIC %sql
# MAGIC create database hive_database

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE int_i18nRegions (id string, iso_country_code string, region_name string, extractDate date, load_date_time timestamp,
# MAGIC  countryCode string, load_date date) USING PARQUET LOCATION 'dbfs:/mnt/natmsdnadlsdatabricks/integration/i18nRegions/incoming/';

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE int_videoCategories (id string, assignable boolean, title string, extractDate date, load_date_time timestamp,
# MAGIC  countryCode string, load_date date) USING PARQUET LOCATION 'dbfs:/mnt/natmsdnadlsdatabricks/integration/videoCategories/incoming/';

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table int_video;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE int_video (id string,
# MAGIC title string,
# MAGIC channelId string,
# MAGIC channelTitle string,
# MAGIC categoryId string,
# MAGIC tags array<string>,
# MAGIC commentCount int,
# MAGIC dislikeCount int,
# MAGIC favoriteCount int,
# MAGIC likeCount int,
# MAGIC viewCount int,
# MAGIC ratingDisabled boolean,
# MAGIC commentDisabled boolean,
# MAGIC publishedAt timestamp,
# MAGIC extractDate date,
# MAGIC load_date_time timestamp,
# MAGIC countryCode string,
# MAGIC load_date date) 
# MAGIC  USING PARQUET LOCATION 'dbfs:/mnt/natmsdnadlsdatabricks/integration/video/incoming/';

# COMMAND ----------

spark.catalog.refreshTable("int_video")

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct load_date_time from int_video order by load_date_time desc;
