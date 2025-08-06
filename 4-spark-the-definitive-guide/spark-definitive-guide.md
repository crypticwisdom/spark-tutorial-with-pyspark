# **Short notes from Spark: the definitive guide:**

## Chapter 1:
...

---

## Chapter 2:
- An action triggers a single job in Spark.
- .read... will triggers a job to read metadata about the data being read, which is the first job spark triggers.
- After applying business transformation to Spark Dataframe, and you use a `.explain` to view the plan for that transformation, it shows you the physical plan (the actual best physical execution plan), if you are okay with the plan you can go ahead and add your action `.write, .show...`, then spark will build a DAG based on the physical plan, sends it to the DAG scheduler which splits it into stages of tasks and these tasks are scheduled across executors.
- There 2 types of operations, `Narrow` (does not require shuffling) and `Wide` (requires data shuffling accross executors).
- Until an action is triggered Spark traverses upward to to build a physical plan based on all transformations done on the Dataframe.
- Spark SQL module provides two interfaces:
  - SQL string interface (spark.sql(...))
  - DataFrame API (df.select, df.filter, etc.)
    The DataFrame API is not the same as "Spark SQL queries," but both belong to the Spark SQL module and offer equivalent functionality.
- There are three kinds of actions:
  - Actions to view data in the console
  - Actions to collect data to native objects in the respective language
  - Actions to write to output data sources.
- Reading data is a transformation, and is therefore a lazy operation. 
- During data source reading, if `inferType` is True, Spark peeks at only a couple of rows of data to try to guess what types each column should be. You also have the option of strictly specifying a schema when you read in data (which we recommend in production scenarios)

---

## Chapter 3: Gives an overview of what's possible in Spark:
- Spark toolsets:
  - Structured Streaming
  - Advance Analytics
  - Libraries and Ecosystem
  - Structured APIs
    - Datasets, Dataframe and SQL
  - Low Level APIs
    - RDDs
    - Distributed Variables
- `spark-submit` does one thing: it lets you send your application code to a cluster and launch it to executre. Upon submission the code runs till it exists (completes the task) or encounter an error.
  - It allows you to specify the resources your application needs as wee as how it should be ran, its command-line arguemants and also the cluster manager to use (Spark standalone, yarn, mesos, K8S)
- Datasets: a type-safe version of Spark's Structured API, for writting statically typed code in Java and Scala. This Dataset API is not available in Python and R because those languages are dynamically typed.
- Structured Streaming:  High level API for stream processing. You can take the same operation that you performed in batch mode using Spark's structured APIs and run them in a streaming fashion. Streaming process reduces latency and allow for incremental processing.
- Transformations on a streaming dataframe are also lazy and requires a streaming action to start the execution of this flow. 
  - Spark reshuffles the data, and the output of that shuffle will have N partitions, where: `N = spark.sql.shuffle.partitions (default = 200)`, If you set it to 5, the shuffled output will have 5 partitions, regardless of the default 200. Spark will create 5 tasks, 1 for each partition.
  - It's not a reduction of existing partitions — it's how Spark re-partitions the data during shuffling.
  - `spark.sql.shuffle.partitions` Controls the number of partitions created by shuffle operations
  - "Practice: because there aren’t many executors on this machine, it’s worth reducing this to 5."

- Low-Level API (RDD): Virtually everything (even the Structured APIs) in Spark is built on top of RDDs. DF operations compiles down to this lower-low tool for convinient and extremely efficient distributed executions, but the RDD operations are not optimized by catalyst optimizer and doesn't have an entry point in the 'SQL and Dataframe' section in the UI. Most of the times developers will write applications with the Structured APIs.

`You may read the rest of the book any way that you prefer, we find that most people hop from area to area as they hear terminology or want to apply Spark to certain problems they’re facing.`
---

## Part 2 -> Chapter 4:




## Part 5 (Streaming) -> Chapter 20: Stream Processing Fundamental

















## Notes:
> The driver **parses** your instructions and builds a plan. The actual **data reading is done by the executors**, in parallel. The Driver does not read the data.
> Only **metadata and logical plans** are held in memory on the driver, the driver does not read data from source. Full datasets **live on executors after they read their partition from source by the executor.**
> DataFrame  = Blueprint (A handle to the logical plan)
> Partition  = A chunk of data read by executor from source
> Task       = Instructions to process a partition (Instruction given to executors to process its partition of data)
> Stage      = A group of parallel tasks
> Driver     = Architect & Dispatcher
> Executor   = Worker


## 🧩 Caching & Memory Notes

- If you call `.cache()` or `.persist()`, Spark stores intermediate results **in executor memory**.
- Without `.cache()`, Spark will **recompute** the data each time it's needed.
- You can **uncache** data using:

    ```python
    df.unpersist()
    ```

- Cached data stays in memory until explicitly **unpersisted** or **evicted** (if memory is full or Spark cluster terminates).
- Caching is an **optimization technique**, but can be **dangerous** in production if misused (memory pressure, stale data, etc.).
- Cached data (Query plan) are stored in memory and can be found on the Spark UI (Storage/Memory section)