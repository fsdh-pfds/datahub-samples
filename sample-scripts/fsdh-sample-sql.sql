-- Databricks notebook source
-- MAGIC %md
-- MAGIC # FSDH Databricks SQL Sample
-- MAGIC *Note: This notebook is a work in progress*
-- MAGIC
-- MAGIC This notebook will use SQL, but Databricks supports programming in Python, Scala, and R as well.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Connecting to storage
-- MAGIC ### Option 1: Using Blob storage
-- MAGIC To read a file in Databricks, you can use the ABFS (Azure Blob File System). For more information on Azure Blob Storage, see: https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction.
-- MAGIC

-- COMMAND ----------

CREATE OR REPLACE TABLE default.fsdh_sample
USING DELTA AS
SELECT *
FROM read_files(
  'abfss://datahub@fsdhprojdw1poc.dfs.core.windows.net/fsdh-sample.csv',
  format => 'csv',
  header => true
);

SELECT * FROM default.fsdh_sample LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Option 2: Mount FSDH storage using a storage key
-- MAGIC Mounting is only available in Databricks using Python or Scala. If you wish you use mounted storage with SQL, run the following code to mount the storage:
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC if any(mount.mountPoint == "/mnt/fsdh" for mount in dbutils.fs.mounts()):
-- MAGIC         dbutils.fs.unmount("/mnt/fsdh")
-- MAGIC
-- MAGIC dbutils.fs.mount(
-- MAGIC   source = spark.conf.get('wasbs_uri'),
-- MAGIC   mount_point = "/mnt/fsdh",
-- MAGIC   extra_configs = {'fs.azure.account.key.' + spark.conf.get('az_storage_name') +'.blob.core.windows.net':dbutils.secrets.get(scope = "datahub", key = "storage-key")})

-- COMMAND ----------

-- MAGIC %md
-- MAGIC You can now return to SQL to access the mounted data.

-- COMMAND ----------

CREATE OR REPLACE TABLE default.fsdh_sample
USING DELTA AS
SELECT *
FROM read_files(
  '/mnt/fsdh/fsdh-sample.csv',
  format => 'csv',
  header => true
);

SELECT * FROM default.fsdh_sample LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Further resources
-- MAGIC For more help with Databricks, consult the [Resources section](https://poc.fsdh-dhsf.science.cloud-nuage.canada.ca/resources/) of the Federal Science DataHub.
