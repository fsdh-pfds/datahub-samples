# Databricks notebook source
# MAGIC %md
# MAGIC # FSDH Databricks Python Sample
# MAGIC *Note: This notebook is a work in progress*
# MAGIC
# MAGIC This notebook will use Python, but Databricks supports programming in SQL, Scala, and R as well.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connecting to storage
# MAGIC ### Option 1: Using Blob storage
# MAGIC To read a file in Databricks, you can use the ABFS (Azure Blob File System). For more information on Azure Blob Storage, see: https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blobs-introduction.
# MAGIC

# COMMAND ----------

abfss = spark.conf.get('abfss_uri')
dbutils.fs.ls(abfss)
df = spark.read.option("header","true").csv(abfss + '/fsdh-sample.csv')
df.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option 2: Mount FSDH storage using a storage key
# MAGIC You can also leverage Azure Key Vault to mount storage and access files through there.

# COMMAND ----------

if any(mount.mountPoint == "/mnt/fsdh" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount("/mnt/fsdh")

dbutils.fs.mount(
  source = spark.conf.get('wasbs_uri'),
  mount_point = "/mnt/fsdh",
  extra_configs = {'fs.azure.account.key.' + spark.conf.get('az_storage_name') +'.blob.core.windows.net':dbutils.secrets.get(scope = "datahub", key = "storage-key")})

dbutils.fs.ls('/mnt/fsdh')
df = spark.read.option("header","true").csv('/mnt/fsdh/fsdh-sample.csv')
df.show(5);

# COMMAND ----------

# MAGIC %md
# MAGIC ## Connecting to a PostgreSQL database
# MAGIC ### Option 1: Using psycopg2 (for reading and writing)
# MAGIC #### Step 1: Install packages

# COMMAND ----------

# MAGIC %pip install psycopg2

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Set up connection details

# COMMAND ----------

HOST="my_host"
DATABASE="my_database"
USER="my_user"
PASSWORD="my_password"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Connect to database

# COMMAND ----------

import psycopg2
from psycopg2 import sql

conn = psycopg2.connect(
    host=HOST,
    database=DATABASE,
    user=USER,
    password=PASSWORD
)
cursor = conn.cursor()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4: Sample operations

# COMMAND ----------

# Creating a table
create_table_query = """
CREATE TABLE IF NOT EXISTS celestial_bodies (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    body_type VARCHAR(50),
    mean_radius_km NUMERIC,
    mass_kg NUMERIC,
    distance_from_sun_km NUMERIC
);
"""
cursor.execute(create_table_query)

# COMMAND ----------

# Inserting data
dummy_data = [
    ('Mercury', 'Planet', 2439.7, 3.3011e23, 57909227),
    ('Venus', 'Planet', 6051.8, 4.8675e24, 108209475),
    ('Earth', 'Planet', 6371.0, 5.97237e24, 149598262),
    ('Mars', 'Planet', 3389.5, 6.4171e23, 227943824),
    ('Jupiter', 'Planet', 69911, 1.8982e27, 778340821),
    ('Europa', 'Moon', 1560.8, 4.7998e22, 670900000), 
    ('Ganymede', 'Moon', 2634.1, 1.4819e23, 670900000),
    ('Ceres', 'Dwarf Planet', 473, 9.3835e20, 413700000),
    ('Pluto', 'Dwarf Planet', 1188.3, 1.303e22, 5906440628)
]

insert_query = """
INSERT INTO celestial_bodies (name, body_type, mean_radius_km, mass_kg, distance_from_sun_km) VALUES (%s, %s, %s, %s, %s);
"""
cursor.executemany(insert_query, dummy_data)
conn.commit()

# COMMAND ----------

# Retrieving data
select_query = "SELECT * FROM celestial_bodies;"
cursor.execute(select_query)

rows = cursor.fetchall()
for row in rows:
    print(row)

# COMMAND ----------

# Displaying data
import pandas as pd
df = pd.read_sql_query(select_query, conn)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5: Close the connection

# COMMAND ----------

conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option 2: Spark on Databricks (read only)
# MAGIC #### Step 1: Set up connection details

# COMMAND ----------

HOST="my_host"
DATABASE="my_database"
USER="my_user"
PASSWORD="my_password"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Read from the database

# COMMAND ----------

url = f"jdbc:postgresql://{HOST}:{5432}/{DATABASE}"
driver = "org.postgresql.Driver"

remote_table = (spark.read
    .format("jdbc")
    .option("driver", driver)
    .option("url", url)
    .option("dbtable", "celestial_bodies")
    .option("user", USER)
    .option("password", PASSWORD)
    .load()
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3: Display the results

# COMMAND ----------

display(remote_table)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Further resources
# MAGIC For more help with Databricks, consult the [Resources section](https://poc.fsdh-dhsf.science.cloud-nuage.canada.ca/resources/) of the Federal Science DataHub.