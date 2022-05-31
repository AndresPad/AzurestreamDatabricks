// Databricks notebook source
// MAGIC %md # <img src ='https://airsblobstorage.blob.core.windows.net/airstream/Asset 275.png' width="50px"> Mounting ADLS Gen2
// MAGIC 
// MAGIC [Service Principal](https://docs.databricks.com/data/data-sources/azure/adls-gen2/azure-datalake-gen2-sp-access.html)
// MAGIC 
// MAGIC [Credential Passthrough](https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/azure/adls-gen2/azure-datalake-gen2-get-started)
// MAGIC 
// MAGIC [Direct access using Access Key](https://docs.microsoft.com/en-us/azure/databricks/security/credential-passthrough/adls-passthrough)
// MAGIC 
// MAGIC [Useful Tutorial](https://www.gerards.tech/blog/connecting-azure-databricks-to-azure-data-lake-store-adls-gen2-part2)

// COMMAND ----------

//FIRST CREATE YOUR KEY VAULT SECRET SCOPE 
//Tutorial: https://docs.microsoft.com/en-us/azure/databricks/security/secrets/secret-scopes

//You need to go here to create your secret Scope
//https://YOURDATABRICKSINSTANCE.azuredatabricks.net#secrets/createScope

// COMMAND ----------

//list the metadata for secrets within the specified scope
//https://docs.databricks.com/dev-tools/databricks-utils.html
dbutils.secrets.list("my-scope")

// COMMAND ----------

//Mounting Data Lake with Service Principal and Azure Key Vault Secret Scope

val storageAccountName = "YOURDATALAKEACCOUNTNAME"
val container = "YOURCONTAINER"
val appID = "YOURSERVICEPRINCIPALID"
val tenantID = "YOURACTIVEDIRECTORYTENANTID"

username and password 

val configs = Map(
  "fs.azure.account.auth.type" -> "OAuth",
  "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
  "fs.azure.account.oauth2.client.id" -> appID,
  "fs.azure.account.oauth2.client.secret" -> dbutils.secrets.get(scope="YOURSECRETSCOPE",key="connection or secret key for the service"),
  "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/YOURACTIVEDIRECTORYTENANTID/oauth2/token"
)

//Example:
//SHOULD LOOK SOMETHING LIKE THIS
// val configs = Map(
//   "fs.azure.account.auth.type" -> "OAuth",
//   "fs.azure.account.oauth.provider.type" -> "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
//   "fs.azure.account.oauth2.client.id" -> appID,
//   "fs.azure.account.oauth2.client.secret" -> dbutils.secrets.get(scope="akv-bck-scope",key="AirsServicePrincipal"),
//   "fs.azure.account.oauth2.client.endpoint" -> "https://login.microsoftonline.com/6bg988bf-xxxx-41af-91ab-XXXXXXXXXXX/oauth2/token"
// )

// COMMAND ----------

//Now create your mount using a Service Principal
dbutils.fs.mount(
  source = "abfss://YOURSTORAGECONTAINER@YOURDATALAKEACCOUNTNAME.dfs.core.windows.net/",
  mountPoint = "/mnt/datalake",
  extraConfigs = configs)

// COMMAND ----------

//1.b Mounting Data Lake with Service Principal and Azure Key Vault Secret Scope with different provider

val storageAccountName = "YOURDATALAKEACCOUNTNAME"
val container = "YOURCONTAINER"
val appID = "YOURSERVICEPRINCIPALID"
val tenantID = "YOURACTIVEDIRECTORYTENANTID"

val configs = Map(
  "dfs.adls.oauth2.access.token.provider.type" -> "ClientCredential",
  "dfs.adls.oauth2.client.id" -> appID,
  "dfs.adls.oauth2.credential" -> dbutils.secrets.get(scope="YOURSECRETSCOPE",key="YOURSERVICEPRINCIPAL"),
  "dfs.adls.oauth2.refresh.url" -> "https://login.microsoftonline.com/YOURACTIVEDIRECTORYTENANTID/oauth2/token"
)

//Example:
//SHOULD LOOK SOMETHING LIKE THIS
// val configs = Map(
//   "dfs.adls.oauth2.access.token.provider.type" -> "ClientCredential",
//   "dfs.adls.oauth2.client.id" -> appID,
//   "dfs.adls.oauth2.credential" -> dbutils.secrets.get(scope="akv-bck-scope",key="AirsServicePrincipal"),
//   "dfs.adls.oauth2.refresh.url" -> "https://login.microsoftonline.com/6bg988bf-xxxx-41af-91ab-XXXXXXXXXXX/oauth2/token"
// )

//Now create your mount using a Service Principal
dbutils.fs.mount(
  source = "adl://YOURDATALAKEACCOUNTNAME.azuredatalakestore.net",
  mountPoint = "/mnt/datalake2",
  extraConfigs = configs
)

// COMMAND ----------

//2.a Accessing Data Lake with Access Key WITHOUT a mount
spark.conf.set(
    "fs.azure.account.key.YOURDATALAKENAME.dfs.core.windows.net",
    dbutils.secrets.get(scope="YOURSECRETSCOPE",key="YOURSECRETKEY"))

//Example:
// spark.conf.set(
//     "fs.azure.account.key.airdatalake.dfs.core.windows.net",
//     dbutils.secrets.get(scope="akv-bck-scope",key="ADLS--AccountKey"))

//Using DBUTILS to list out all files and folders in mount
dbutils.fs.ls("abfss://CONTAINERNAME@YOURDATALAKENAME.dfs.core.windows.net/")

// COMMAND ----------

//2.b Mounting Data Lake WITH Access Key through Secret Scope
//Please note: - If you are using the original Windows Azure Storage Blob (WASB) driver it is recommended to use ABFS with ADLS due to greater efficiency with directory level operations.
//https://medium.com/microsoftazure/securing-access-to-azure-data-lake-gen2-from-azure-databricks-8580ddcbdc6
//https://docs.databricks.com/data/data-sources/azure/azure-storage.html#language-scala

dbutils.fs.mount(
  source = "wasbs://YOURSTORAGECONTAINER@YOURDATALAKENAME.blob.core.windows.net",
  mountPoint = "/mnt/datalake3",
  extraConfigs = Map("fs.azure.account.key.YOURDATALAKENAME.blob.core.windows.net" -> dbutils.secrets.get(scope = "YOURSECRETSCOPE", key = "YOURSECRETKEY")))

//Example:
// dbutils.fs.mount(
//   source = "wasbs://landingcontainer@airdatalake.blob.core.windows.net",
//   mountPoint = "/mnt/datalake3",
//   extraConfigs = Map("fs.azure.account.key.airdatalake.blob.core.windows.net" -> dbutils.secrets.get(scope = "akv-bck-scope", key = "ADLS--AccountKey")))

// COMMAND ----------

//2.c Mounting Data Lake WITH INLINE Access Key
//Remember to get your primary or secondary storage access key
//YOU CAN DO THIS BUT WE RECOMMEND USING SECRET SCOPES WITH KEY VAULT AND SERVICE PRINCIPALS INSTEAD
val configs = Map(
  "fs.azure.account.key.<YOURDATALAKENAME>.blob.core.windows.net" -> "YOURSTORAGEACCOUNTPRIMARYKEYXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX=="
)

dbutils.fs.mount(
  source = "wasbs://YOURSTORAGECONTAINER@YOURDATALAKENAME.blob.core.windows.net",
  mountPoint = "/mnt/datalake4",
  extraConfigs = configs)

// COMMAND ----------

//3.a Mount an Azure Data Lake Storage Gen2 with CREDENTIAL PASSTHROUGH
//Make sure you enable credential passthrough on the Cluster. So go to the cluster and make sure the checkbox for credential passthrough is checked in the advanced options section of the cluster
//https://docs.microsoft.com/en-us/azure/databricks/security/credential-passthrough/adls-passthrough

val configs = Map(
  "fs.azure.account.auth.type" -> "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class" -> spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
)

dbutils.fs.mount(
  source = "abfss://YOURSTORAGECONTAINER@YOURDATALAKENAME.dfs.core.windows.net/",
  mountPoint = "/mnt/datalake5",
  extraConfigs = configs)

//Example:
// dbutils.fs.mount(
//   source = "abfss://landingcontainers@airdatalake.dfs.core.windows.net/",
//   mountPoint = "/mnt/datalake5",
//   extraConfigs = configs)

// COMMAND ----------

// MAGIC %md ####Get list of mount attached to the workspace using filesystem command

// COMMAND ----------

// MAGIC %fs ls /mnt/

// COMMAND ----------

// MAGIC %md ####Get list of mount attached to the workspace using DBUTILS command

// COMMAND ----------

display(
  dbutils.fs.ls("/mnt/")
)

// COMMAND ----------

// MAGIC %md ####Get list of files and folders inside a mount

// COMMAND ----------

display(
  dbutils.fs.ls("/mnt/datalake")
)

// COMMAND ----------

// MAGIC %md ####Unmount a Mount

// COMMAND ----------

dbutils.fs.unmount("/mnt/datalake6")
