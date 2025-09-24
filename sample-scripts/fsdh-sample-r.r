# Databricks notebook source
# MAGIC %md
# MAGIC # FSDH Databricks R Sample
# MAGIC *Note: This notebook is a work in progress*
# MAGIC
# MAGIC This notebook will use R, but Databricks supports programming in SQL, Scala, and Python as well.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connecting to storage
# MAGIC ### Option 1: Using Blob storage
# MAGIC To read a file in Databricks, you can use the ABFS (Azure Blob File System). For more information on Azure Blob Storage, see: https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction. We use the sparklyr library to facilitate this in R.
# MAGIC

# COMMAND ----------

library(sparklyr)
config <- spark_config()
config$sparklyr.databricks.connect <- TRUE

sc <- spark_connect(
  method = "databricks",
  config = config
)
abfss <- "abfss://datahub@fsdhprojdw1poc.dfs.core.windows.net"
df <- spark_read_csv(
  sc,
  name = "df",
  path = paste0(abfss, "/fsdh-sample.csv"),
  header = TRUE
)
head(df, 5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option 2: Mount FSDH storage using a storage key
# MAGIC Mounting is only available in Databricks using Python or Scala. If you wish you use mounted storage with R, run the following python code to mount the storage:

# COMMAND ----------

# MAGIC %python
# MAGIC if any(mount.mountPoint == "/mnt/fsdh" for mount in dbutils.fs.mounts()):
# MAGIC         dbutils.fs.unmount("/mnt/fsdh")
# MAGIC
# MAGIC dbutils.fs.mount(
# MAGIC   source = spark.conf.get('wasbs_uri'),
# MAGIC   mount_point = "/mnt/fsdh",
# MAGIC   extra_configs = {'fs.azure.account.key.' + spark.conf.get('az_storage_name') +'.blob.core.windows.net':dbutils.secrets.get(scope = "datahub", key = "storage-key")})

# COMMAND ----------

# MAGIC %md
# MAGIC You can now return to R to access the mounted data.

# COMMAND ----------

library(sparklyr)

# Connect to Spark
sc <- spark_connect(method = "databricks")

# Read CSV file from mount
df <- spark_read_csv(
  sc,
  name = "fsdh_sample",
  path = "/mnt/fsdh/fsdh-sample.csv",
  header = TRUE
)

# Show first 5 rows
df %>% head(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Further resources
# MAGIC For more help with Databricks, consult the [Resources section](https://poc.fsdh-dhsf.science.cloud-nuage.canada.ca/resources/) of the Federal Science DataHub.