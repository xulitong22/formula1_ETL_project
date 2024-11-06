# Databricks notebook source
# MAGIC %md
# MAGIC ## Mount ADLS using Service Principal
# MAGIC 1. Get client_id, tenant_id, client_secret from Azure key vault
# MAGIC 2. Set spark config with App/Client id, Directory/Tenant Id & Secret
# MAGIC 3. Call file system utility mount to mount the storage
# MAGIC 4. Explore other file system utilities related to mount(list all mounts, unmount)

# COMMAND ----------

client_id = dbutils.secrets.get(scope='formula1proj-scope', key='formula1-app-client-id')
tenant_id = dbutils.secrets.get(scope='formula1proj-scope', key='formula1-app-tenant-id')
client_secret = dbutils.secrets.get(scope='formula1proj-scope', key='formula1-app-client-secret')

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# COMMAND ----------

dbutils.fs.mount(
  source = "abfss://demo@formula1proj123.dfs.core.windows.net/",
  mount_point = "/mnt/formula1proj123/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1proj123/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formula1proj123/demo"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

#dbutils.fs.unmount("/mnt/formula1proj123")