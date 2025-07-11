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
    SELECT DISTINCT(Product_SKU) as Product_SKU, Product_Category_Code
    FROM PARQUET.`abfss://silver@azstorageaccountstunity.dfs.core.windows.net/electronicssales`
    """
)

df_src.display()

# COMMAND ----------

if not spark.catalog.tableExists('elesales_catalog.gold.dim_product'):
    df_sink = spark.sql(
        '''
        SELECT 1 as dim_product_key, Product_SKU, Product_Category_Code
        FROM PARQUET.`abfss://silver@azstorageaccountstunity.dfs.core.windows.net/electronicssales`
        WHERE 1=0
        '''
    )
else:
    df_sink = spark.sql(
        '''
        SELECT dim_product_key, Product_SKU, Product_Category_Code 
        FROM elesales_catalog.gold.dim_product
        '''
    )

# COMMAND ----------

df_sink.display()

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src.Product_SKU == df_sink.Product_SKU, 'left').select(df_src.Product_SKU, df_src.Product_Category_Code, df_sink.dim_product_key)


# COMMAND ----------

df_filter.display()

# COMMAND ----------

df_filter_old = df_filter.filter(df_filter.dim_product_key.isNotNull())

# COMMAND ----------

df_filter_old.display()

# COMMAND ----------

df_filter_new = df_filter.filter(df_filter.dim_product_key.isNull()).select('Product_SKU', 'Product_Category_Code')

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------
df_filter_new.display()

if (incremental_flag == '0'): 
    max_value = 1
else:
    if spark.catalog.tableExists('elesales_catalog.gold.dim_product'):
        max_value_df = spark.sql("SELECT max(dim_product_key) FROM elesales_catalog.gold.dim_product")
        max_value = max_value_df.collect()[0][0]

# COMMAND ----------

if spark.catalog.tableExists('elesales_catalog.gold.dim_product'):
    max_value_df = spark.sql("SELECT max(dim_product_key) FROM elesales_catalog.gold.dim_product")
    max_value = max_value_df.collect()[0][0]
else:
    max_value = 1

# COMMAND ----------

from pyspark.sql import functions as F
df_filer_new = df_filter_new.withColumn('dim_product_key', max_value + F.monotonically_increasing_id())

# COMMAND ----------

df_filter_new = df_filter_new.withColumn('dim_product_key', max_value + F.monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists('elesales_catalog.gold.dim_product'):
    delta_table = DeltaTable.forPath(spark, "abfss://gold@azstorageaccountstunity.dfs.core.windows.net/dim_model")
    # update when the value exists
    # insert when new value 
    delta_table.alias("target").merge(df_final.alias("source"), "target.dim_product_key = source.dim_product_key")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

# Initial RUN 
else: # no table exists
    df_final.write.format("delta")\
        .mode("overwrite")\
        .option("path", "abfss://gold@azstorageaccountstunity.dfs.core.windows.net/dim_product")\
        .saveAsTable("elesales_catalog.gold.dim_product")
# COMMAND ----------

%sql
select * from elesales_catalog.gold.dim_product
