# PySpark — Data Skew, Quick Notes

<img height="200" alt="image" src="https://github.com/user-attachments/assets/1fe399d9-0af4-428a-8e66-e8691b398057" />

## What it is

Data unevenly split across partitions — one or more partitions hold disproportionately more rows than the rest.



## Why it happens

- **Aggregation (`groupBy`):** one key has far more rows than others (e.g. 12 countries at ~100 records each, one at 1000).

<img height="800" alt="image" src="https://github.com/user-attachments/assets/e798a4f4-ea42-4255-99dc-1d719fe969f5" />

- **Join:** one join key has far more matching rows than others (e.g. one `product_id` or `customer_id` dominates the dataset).

<img height="346" alt="image" src="https://github.com/user-attachments/assets/3a33c5b2-8b66-46ef-82d3-085f86ba27cb" />


## Why it's bad

- A **stage only finishes once every task in it finishes** — so stage runtime = the slowest task's time, not the average. 11 light tasks finish in seconds; the stage still waits on the 1 skewed task (video's example: starts at minute 12, runs to 1.5 hrs).
- **Idle cores** — every other core sits idle once its task is done, while you're still paying for that capacity.
- **OOM / disk spill risk** — the skewed partition holds far more data than a task's memory allowance handles well; it can spill to disk (slow — write then read back) or fail with OOM. Only the skewed task is affected; identical config handles the other partitions fine.

## How to detect it (Spark UI)

| Where | What to look for |
|---|---|
| **Task list for the stage** | Sort by duration — one task starts very late / runs far longer than the rest |
| **Event timeline** (Stages tab) | One disproportionately long bar among otherwise similar-length task bars |
| **Summary metrics table** | Large gap between min and max task duration (video's example: 5 sec vs 31 min) |

<img height="842" alt="image" src="https://github.com/user-attachments/assets/393be316-b041-4e7f-a402-d403a31ca24e" />


## Programmatic check

```python
from pyspark.sql import functions as F

df.withColumn("partition_id", F.spark_partition_id()) \
  .groupBy("partition_id") \
  .count() \
  .show()
```

Row counts wildly uneven across `partition_id` = skew, confirmed without opening the UI.
