# Batch and Streaming Pipelines using Youtube Data API on Azure
**Disclaimer**
All sensitive information (e.g tenant id, resource id, user name etc) has been redacted

 **Disclaimer**
 Opinions expressed are solely my own and do not express the views or opinions of my employer

The code consists of three parts
* Batch Ingestion of Youtube Data
* Streaming Ingestion of Youtube Data
* Serving layer using internal databricks hive metastore and unmanaged tables

**The following diagram shows the end to end architecture**
![image](https://user-images.githubusercontent.com/78700027/126080681-fe57e9a0-97b6-457a-af40-f9f8bab058de.png)

**The following diagram shows the end to end data flow**

![image](https://user-images.githubusercontent.com/78700027/126081070-1e87c343-8c7e-47b1-8a09-3f545c9a4788.png)

**Technologies used**
* GCP API&Services
* Youtube Data API
* Databricks workspace in private vnet
* Databricks connect
* Keyvault
* Azure Datafactory
* Azure Datalake Service gen 2
* Azure Blob Storage
* Azure Event Hub
* Azure VM


## Batch Ingestion of Youtube Data
There are 3 APIs being called
1. i18nRegions
1. VideoCategories
1. Video

**i18nRegions and VideoCategories API**

The i18nRegions and VideoCategories are called directly using Python Request library and ingested to landing folder as json. The files are then converted to parquet in staging and further cleaned and restructured in integration

**Video API**

Video API is called through Python Request Library and passed through the event hub captured into a default landing directory configured in the Event Hub capture. Event hub default file type is avro. The avro file is then moved to landing directory. The content of the API call is included in the Body column and is in binary form. The code converts the binary column to json string and convert it to struct column in the parquet file. The parquet file is loaded to staging raw. The struct Body column is flatten out again in staging processed and loaded to a parquet file. The file is further cleaned and restructured in integration

The STM for the batch layer can be found under /STM.xlsx

## Serving layer

The code leverage databricks internal metastore and unmanaged tables to serve the integration layer to the consumers. Consumers can query the hive table using databricks notebook. 

## Streaming layer

Video API is passed through Event Hub and consumed by databricks notebook. The databricks notebook then output the aggregated tables to console

## Databricks connect
Databricks connect is used to create the libraries and jobs configuration used by the batch ingestion. Once the base libraries are completed, they are packaged into a wheel file and is copied to the dbfs. The whl file is then installed as a cluster library

The configs are moved to azure blob storage

We use visual studio and github to version control the code and configs

## Databricks and ADF
Once the development is done locally using VS Code and Databricks Connect, the blob storage containing the configs are mounted to dbfs. a caller notebook <notebook_workflow> is created to call the libraries, configs and logging. Databricks widget is used to pass parameters into the notebook

The notebook is called from Azure Data Factory. Parameters are passed into each activity in the adf and determines which if block gets executed
