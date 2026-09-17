# PySpark — Partitioning, Quick Notes

<img height="300" alt="image" src="https://github.com/user-attachments/assets/98968a30-8654-470f-a72d-61bcfe40574c"/>

**Partition** = a chunk of data. 1 partition = 1 task = 1 core. All cores across all executors run in parallel (5 executors × 4 cores = 20 at once).

<img width="281" height="281" alt="image" src="https://github.com/user-attachments/assets/0d81406f-5885-44a4-8f42-dbdfc5781334" />

## Two separate settings

| Moment | Setting | Default | Controls |
|---|---|---|---|
| Reading files | `spark.sql.files.maxPartitionBytes` | 128 MB | Max partition size at read time |
| After a shuffle (join/groupBy/distinct/orderBy) | `spark.sql.shuffle.partitions` | 200 | Partition count of shuffled output |

They apply at different moments — never "both conditions together." No shuffle in the pipeline (e.g. read → filter/select → write) → the 200 setting is never invoked; input partitions carry straight through to the write.

## Problems

- **Too few/large:** idle cores, slow job, OOM/spill risk; one oversized partition = **skew**
- **Too many/small:** scheduling overhead, **small file problem** on write

Shuffles are caused by wide transformations, not by partitions — they're the expensive part; shuffle-partition count is what you tune afterward.

## Tuning shuffle partitions

`num_partitions = shuffle_write_size / target_size` — target **1–200 MB/partition**. Get shuffle write size from Spark UI.

<img width="666" height="1291" alt="image" src="https://github.com/user-attachments/assets/fc1a3672-b13b-40ee-8530-5610fcc5c005" />


- **300 GB, 20 cores:** 200 partitions → 1.5 GB each (too big) → target 200 MB → **1500 partitions**
- **50 MB, 12 cores:** 200 partitions → 250 KB each (too small) → target a size (e.g. 10 MB) gives only 5 partitions, leaving cores idle → **matching core count (12) is preferred**, ~4.2 MB/core, full utilization

Still slow after tuning → likely skew, not fixed by partition count alone.

## `coalesce` vs `repartition`

| | Shuffle | Can increase? | Use |
|---|---|---|---|
| `coalesce(n)` | No | No (decrease only) | Cheaply cut file count before writing |
| `repartition(n)` | Yes | Yes | Rebalance, or set files/folder before `partitionBy` |

## `partitionBy` (on-disk folders, different from in-memory partitions above)

`df.write.partitionBy("date")` → one folder per value, enables **partition pruning** on filters.

- Column choice: **low-to-medium cardinality** (not `customer_id`, not a constant) + **a column queries actually filter on**
- Multi-level: `partitionBy("date", "hour")` nests in the order given — most-filtered column first
- Files per folder: `repartition(n)` before `partitionBy` works (forces a shuffle); `coalesce(n)` before it does nothing if the data was already 1 partition, since coalesce can only decrease


## Dynamic Partition Pruning (DPP)

Same underlying mechanism as partition pruning above — skip folders that can't match. The only difference is **where the filter values come from**.

| | Static pruning | Dynamic pruning |
|---|---|---|
| Filter value known | At query-compile time (hardcoded, or passed in by the user/BI tool) | Only at runtime, computed from the other side of a join |
| Typical case | `WHERE listen_date = '2023-06-04'` | Join: filter the small side first, use its resulting values to prune the large side |

**Mechanism:** in a join, Spark evaluates the filtered, smaller side first (the side small enough to broadcast). Whatever values survive that filter are then used **as if they were a static filter** on the other, larger side — but only if that larger side is already physically partitioned by the joined column. Spark reuses the broadcast exchange to do this, so it doesn't cost a second scan.

**Automatic** — on by default (`spark.sql.optimizer.dynamicPartitionPruning.enabled = true`). No hint or API call needed; Spark detects the pattern and inserts a `dynamic pruning expression` into the physical plan itself.

**Gotcha:** requires the large table to already be partitioned on the join key. If it isn't — or the join is on a different, unpartitioned column — there are no folders to skip, and Spark falls back to a full scan.
