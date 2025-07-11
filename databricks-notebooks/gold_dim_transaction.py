#TRANSACTION
# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Create Flag Parameter

# COMMAND ----------

# incremental_flag with default value 0
dbutils.widgets.text('incremental_flag', 'o')

# COMMAND ----------

# get the value of the incremental_flag (will be used for incremental load and intial load)
# incremental flag is a string
# incremental_flad is used to determine if the data is incremental or initial load
incremental_flag = dbutils.widgets.get('incremental_flag')
print(incremental_flag)

# COMMAND ----------

# MAGIC %md 
# MAGIC # Creating Dimenion Model 

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Fetch Relative Columns

# COMMAND ----------

df_src = spark.sql(
    """
    SELECT DISTINCT Transaction_ID
    FROM PARQUET.`abfss://silver@azstorageaccountstunity.dfs.core.windows.net/electronicssales`
    """
)

df_src.display()

# COMMAND ----------

if not spark.catalog.tableExists('elesales_catalog.gold.dim_transaction'):
    df_sink = spark.sql(
        '''
        SELECT 1 as dim_transaction_key
        FROM PARQUET.`abfss://silver@azstorageaccountstunity.dfs.core.windows.net/electronicssales`
        WHERE 1=0
        '''
    )
else:
    df_sink = spark.sql(
        '''
        SELECT dim_transaction_key
        FROM elesales_catalog.gold.dim_transaction
        '''
    )
# COMMAND ----------

df_sink.display()

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src.Transaction_ID == df_sink.Transaction_ID, 'left').select(df_src.Transaction_ID, df_sink.dim_transaction_key)


# COMMAND ----------

df_filter.display()

# COMMAND ----------

df_filter_old = df_filter.filter(df_filter.dim_transaction_key.isNotNull())

# COMMAND ----------

df_filter_old.display()

# COMMAND ----------

df_filter_new = df_filter.filter(df_filter.dim_transaction_key.isNull()).select('Transaction_ID')

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------
df_filter_new.display()

# COMMAND ----------

if (incremental_flag == '0'): 
    max_value = 1
else:
    if spark.catalog.tableExists('elesales_catalog.gold.dim_transaction'):
        max_value_df = spark.sql("SELECT max(dim_transaction_key) FROM elesales_catalog.gold.dim_transaction")
        max_value = max_value_df.collect()[0][0]
# COMMAND ----------

if spark.catalog.tableExists('elesales_catalog.gold.dim_transaction'):
    max_value_df = spark.sql("SELECT max(dim_transaction_key) FROM elesales_catalog.gold.dim_transaction")
    max_value = max_value_df.collect()[0][0]
else:
    max_value = 1

# COMMAND ----------

from pyspark.sql import functions as F
df_filer_new = df_filter_new.withColumn('dim_transaction_key', max_value + F.monotonically_increasing_id())
# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_transaction_key', max_value + F.monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('elesales_catalog.gold.dim_transaction'):
    delta_table = DeltaTable.forPath(spark, "abfss://gold@azstorageaccountstunity.dfs.core.windows.net/dim_transaction")
    # update when the value exists
    # insert when new value 
    delta_table.alias("target").merge(df_final.alias("source"), "target.dim_transaction_key = source.dim_transaction_key")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

# Initial RUN 
else: # no table exists
    df_final.write.format("delta")\
        .mode("overwrite")\
        .option("path", "abfss://gold@azstorageaccountstunity.dfs.core.windows.net/dim_transaction")\
        .saveAsTable("elesales_catalog.gold.dim_transaction")
# COMMAND ----------

%sql
select * from elesales_catalog.gold.dim_transaction



















if not spark.catalog.tableExists('elesales_catalog.gold.dim_Transaction'):
    df_sink = spark.sql(
        '''
        SELECT 1 as dim_vendor_key, Transaction_ID
        FROM PARQUET.`abfss://silver@azstorageaccountstunity.dfs.core.windows.net/electronicssales`
        WHERE 1=0
        '''
    )
else:
    df_sink = spark.sql(
        '''
        SELECT dim_vendor_key, Transaction_ID 
        FROM elesales_catalog.gold.dim_Transaction
        '''
    )






df_filter = df_src.join(df_sink, df_src.Transaction_ID == df_sink.Transaction_ID, 'left').select(df_src.Transaction_ID, df_sink.dim_vendor_key)


