#!/usr/bin/env python
# coding: utf-8

# ## 06_z_ordering
# 
# 
# 

# In[1]:


from pyspark.sql import functions as F
from delta.tables import DeltaTable


# ## Z-Ordering

# In[3]:


src_file = "abfss://testcontainer@storageinswa01us2dev.dfs.core.windows.net/delta_lake_tutorial/raw_data/invoices_201_99457.parquet"

df = spark.read.format('parquet').load(src_file).select("customer_id", 'category', 'price', 'quantity', 'invoice_date')

display(df.limit(3))


# generating lots of rows for simulating Z-order

# In[5]:


df_union = df
expected_rows = 20e6

while df_union.count() <= expected_rows:
    df_union = df_union.union(df_union)
    print("Row count: ", df_union.count())

print("Final Row count:", df_union.count())


# In[10]:


target_file = "abfss://testcontainer@storageinswa01us2dev.dfs.core.windows.net/delta_lake_tutorial/optimize/z_ordering_src_file"
df_union.write.mode('overwrite').format('delta').save(target_file)


# ## Query table without Z-ordering
# 
# Takes around `18` seconds

# In[12]:


df_large_table = spark.read.format('delta').load(target_file)
agg = df_large_table.filter(F.col("customer_id")==201).groupBy('category').agg(F.sum(F.col('price') * F.col("quantity")).alias("total_sales"))
display(agg.limit(5))


# ## Applying Z-Ordering
# 
# ```python
# delta_table.optimize().executeZOrderBy('customer_id')
# ```
# 
# - `optimize`: Compacts small files into fewer, appropriately-sized ones — same
#   bin-packing mechanism as a plain `OPTIMIZE`, just running **together with**
#   Z-Ordering in one operation (they're always used in tandem, not `optimize`
#   running as a separate first step then Z-Ordering after).
# - `Z-Ordering`: **Not** a simple sort by `customer_id`. It's a technique that
#   **colocates similar values of `customer_id` into the same files** — rows
#   with nearby/related `customer_id` values end up packed together — so that
#   queries filtering on `customer_id` can **skip entire files** whose min/max
#   range doesn't overlap the filter, instead of scanning every file.
# 
# - Improved query time from 18 to 10 seconds

# In[14]:


delta_table = DeltaTable.forPath(spark, target_file)
delta_table.optimize().executeZOrderBy('customer_id')


# In[16]:


df_large_table = spark.read.format('delta').load(target_file)
agg = df_large_table.filter(F.col("customer_id")==201).groupBy('category').agg(F.sum(F.col('price') * F.col("quantity")).alias("total_sales"))
display(agg.limit(5))


# ## Z-Ordering in Hive Style Partitions

# In[20]:


target_file_v2 = "abfss://testcontainer@storageinswa01us2dev.dfs.core.windows.net/delta_lake_tutorial/optimize/z_ordering_partitioned"
df.write.format('delta').partitionBy('invoice_date').mode('overwrite').save(target_file_v2)


# In[23]:


df_new_data = df.filter(F.col("invoice_date")=="2021-07-04").withColumn("invoice_date", F.lit("2026-09-16").cast("date"))
display(df_new_data.limit(10))


# In[24]:


df_new_data.write.mode('append').partitionBy('invoice_date').format('delta').save(target_file_v2)


# In[25]:


delta_table_v2 = DeltaTable.forPath(spark, target_file_v2)
delta_table_v2.optimize().where("invoice_date = '2026-09-16'").executeZOrderBy('customer_id')

