# Databricks notebook source
# MAGIC %md 
# MAGIC # CREATE FACT TABLE

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Reading Silver Data

# COMMAND ----------

df_silver =spark.sql("SELECT * from PARQUET.`abfss://silver@azstorageaccountstunity.dfs.core.windows.net/electronicssales`") 

# COMMAND ----------

df_silver.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading all the DIMs

# COMMAND ----------

df_product = spark.sql("SELECT * elesales_catalog.gold.dim_model")

df_store = spark.sql("SELECT * elesales_catalog.gold.dim_store")

df_transaction = spark.sql("SELECT * elesales_catalog.gold.dim_transaction")

df_vendor = spark.sql("SELECT * elesales_catalog.gold.dim_vendor")

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Brining Keys to the FACT table

# COMMAND ----------

df_fact = df_silver.join(df_product, df_silver.Product_SKU==df_product.Product_SKU, how='left') \
    .join(df_store, df_silver.Store_ID==df_store.Store_ID, how='left') \
    .join(df_transaction, df_silver.Transaction_ID==df_transaction.Transaction_ID, how='left') \
    .join(df_vendor, df_silver.Vendor_ID==df_vendor.Vendor_ID, how='left')\
    .select(df_silver.Revenue, df_silver.Units_Sold, df_product.dim_model_key, df_store.dim_store_key, df_transaction.dim_transaction_key, df_vendor.dim_vendor_key)

# COMMAND ----------

df_fact.display()

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Writing Fact Table

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('factsales'): 
    deltatable = DeltaTable.forName(spark, 'elesales_catalog.gold.factsales')

    deltatable.alias('trg').merge(df_fact.alias('src'), 'trg.dim_branch_key = src.dim_branch_key and trg.dim_dealer_key = src.dim_dealer_key and trg.dim_model_key = src.dim_model_key and trg.dim_date_key = src.dim_date_key')\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

else: 
    df_fact.write.format('delta')\
            .mode('Overwrite')\
            .option("path", "abfss://gold@azstorageaccountstunity.dfs.core.windows.net/factsales")\
            .saveAsTable('elesales_catalog.gold.factsales')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM elesales_catalog.gold.factsales

# COMMAND ----------
