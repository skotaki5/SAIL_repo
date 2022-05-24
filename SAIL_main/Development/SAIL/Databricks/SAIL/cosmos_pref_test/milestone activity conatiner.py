# Databricks notebook source
scope = 'key-vault-secrets'
cosmosEndpoint = dbutils.secrets.get(scope,"cosmosEndpoint")
cosmosMasterKey = dbutils.secrets.get(scope,"cosmosMasterKey")
cosmosDatabaseName = "SAIL"
cosmosContainerName = "digital_summary_milestone_activity"

cfg = {
  "spark.cosmos.accountEndpoint" : cosmosEndpoint,
  "spark.cosmos.accountKey" : cosmosMasterKey,
  "spark.cosmos.database" : cosmosDatabaseName,
  "spark.cosmos.container" : cosmosContainerName,
    "spark.cosmos.read.customQuery": "SELECT * FROM c"
}

cfg2 = {
  "spark.cosmos.accountEndpoint" : cosmosEndpoint,
  "spark.cosmos.accountKey" : cosmosMasterKey,
  "spark.cosmos.database" : "SAIL_PREF_TEST",
  "spark.cosmos.container" : cosmosContainerName
}


df = spark.read.format("cosmos.oltp").options(**cfg)\
    .option("spark.cosmos.read.inferSchema.enabled","true").load()
df=df.filter(df.is_deleted==0)
df.write.format("cosmos.oltp").options(**cfg2).mode("APPEND").save()

# COMMAND ----------

from pyspark.sql.functions import concat,lit
df=df.withColumn("UpsOrderNumber",concat(df.UpsOrderNumber,lit('_1')))
df=df.withColumn("id",concat(df.id,lit('_1')))
df.write.format("cosmos.oltp").options(**cfg2).mode("APPEND").save()


# COMMAND ----------

df=df.withColumn("UpsOrderNumber",concat(df.UpsOrderNumber,lit('_2')))
df=df.withColumn("id",concat(df.id,lit('_2')))
df.write.format("cosmos.oltp").options(**cfg2).mode("APPEND").save()

# COMMAND ----------

df=df.withColumn("UpsOrderNumber",concat(df.UpsOrderNumber,lit('_3')))
df=df.withColumn("id",concat(df.id,lit('_3')))
df.write.format("cosmos.oltp").options(**cfg2).mode("APPEND").save()