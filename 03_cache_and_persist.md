# PySpark — Cache & Persist, Quick Notes

<img height="200" alt="image" src="https://github.com/user-attachments/assets/673635dc-2fde-42c6-82dd-ae8afc48f0fc" />

## The problem

Lazy evaluation means a DataFrame isn't computed when defined, only when an action runs. If two downstream DataFrames both build on the same base DataFrame, calling an action on each one replays the **entire lineage from scratch**, including the base, every time.

```python
df_base = (df.filter(F.col("city") == "Boston")
             .withColumn("customer_group", ...)
             .select(...))

df1 = df_base.withColumn("test1", ...).withColumn("birth_year", ...)
df2 = df_base.withColumn("test2", ...).withColumn("birth_month", ...)

df1.show()   # re-reads file, re-filters, re-creates customer_group
df2.show()   # does all of that again, independently
```

Without caching, `df_base`'s work (read, filter, `customer_group`) gets computed twice — once per action — even though it's identical both times.

## The fix

`.cache()` stores the computed result of `df_base` so later actions skip recomputing it. Confirmed in the query plan: `Scan parquet` + `Filter` disappear from `df1`/`df2`'s plans, replaced by a single `InMemoryTableScan` that already has `customer_group`. Only what's added *after* the base still computes. In the Spark UI, `Scan parquet` input size drops to 0 for the second and third actions.

## `cache()` vs `persist()`

`cache()` is `persist()` with the storage level hardcoded to the default — not two separate mechanisms.

```python
df.cache()                                       # shorthand
df.persist()                                     # identical, same default level
df.persist(StorageLevel.MEMORY_AND_DISK_DESER)   # explicit level
```

## Storage levels

| Level | Where | Format | Notes |
|---|---|---|---|
| **`MEMORY_AND_DISK_DESER`** (default) | Memory, spills to disk | Deserialized (JVM objects) | Fast reads, no decode cost |
| `MEMORY_ONLY` | Memory only | Deserialized | No disk fallback — data that doesn't fit is dropped |
| `DISK_ONLY` | Disk only | Serialized | |
| `MEMORY_AND_DISK` | Memory, spills to disk | Serialized | |

Add `_2` / `_3` etc. (e.g. `MEMORY_ONLY_2`) to replicate across the cluster for fault tolerance.

**Trade-off:** deserialized = more memory, zero CPU cost to read. Serialized = less memory, CPU cost every read to decode. Pick based on whether the job is memory-constrained or CPU-constrained.

## When to cache

Only pays off if the DataFrame is **reused multiple times downstream**. Caching something used once just adds overhead for no benefit.

---

## Practical example — aggregated sales, split by district and state

**Scenario:** an aggregated sales table needs to be grouped two different ways — by district and by state — with each result written separately to the gold layer.

```python
from pyspark.sql import functions as F

sales_agg = (spark.read.parquet("silver/sales_agg")
                   .filter(F.col("is_active") == True))
             # some shared cleanup/filtering here — this is the reused base

sales_agg.cache()   # or .persist() — this DataFrame feeds two separate group-bys

district_sales = (sales_agg.groupBy("district")
                            .agg(F.sum("amount").alias("total_sales")))

state_sales = (sales_agg.groupBy("state")
                         .agg(F.sum("amount").alias("total_sales")))

district_sales.write.mode("overwrite").parquet("gold/district_sales")
state_sales.write.mode("overwrite").parquet("gold/state_sales")

sales_agg.unpersist()   # free it once both writes are done
```

**Why caching helps here:** `district_sales` and `state_sales` are two independent branches built on the same `sales_agg` base, each triggered by its own action (`.write`). Without caching, the read + filter on `sales_agg` runs twice — once per write. Caching it means that work happens once, and both `groupBy`s branch off the cached in-memory copy.

**Sizing note:** if `sales_agg` is large relative to available executor memory, `MEMORY_AND_DISK` (serialized) is a safer choice than the default `MEMORY_AND_DISK_DESER` — it costs some CPU to deserialize on each read, but avoids holding two large uncompressed copies in memory space that's already tight. Calling `.unpersist()` after both writes complete frees that memory for the rest of the job instead of holding it until the session ends.

<img height="800" alt="image" src="https://github.com/user-attachments/assets/853503e4-5473-4665-94df-24dce18387e7" />
