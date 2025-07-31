# 1. Guide to Apache Spark

---

# Spark Fundamentals:

## 📌 1. **Introduction to Apache Spark**

Apache Spark is an open-source distributed data processing engine for large data analytics and machine learning tasks. It provides fast in-memory computation capabilities suitable for batch and streaming workloads.

**Spark Core vs. Apache Spark**: Spark Core is the foundational engine that handles task scheduling, memory management, fault recovery, and the RDD API. **Apache Spark** refers to the entire ecosystem, including Spark Core plus higher-level modules (Spark SQL, MLlib, Streaming, GraphX).


### **Spark Modules**

Think of Spark’s “modules” not as totally separate engines, but as **bundled libraries** or **components** that all run on top of the same core execution engine (Spark Core). 

- **Spark Core:**  is the **foundational engine** of Apache Spark. Foundation handling basic I/O, task scheduling, memory management, and recovery.
- **Spark SQL:** DataFrame/Dataset abstraction, SQL semantics, schema support, and optimization via Catalyst.
- **Structured Streaming:** DataFrame-based streaming queries.
- **MLlib:** ML algorithms and pipeline utilities.
- **GraphX:** Graph computation engine using RDDs.
- **SparkR & PySpark:** Language-specific wrappers allowing R and Python users to interact with Spark.


### Batch vs Streaming:
#### **What is Batch Processing?**:
Batch processing is when Spark reads a large, fixed dataset (bounded dataset), processes it all at once, and outputs results.
Once the data is processed, the job ends. That's batch!

**Example:**
- Datawarehouse loading
- Monthly Report Generation
- Monthly billing

> Spark Module Used: Spark Core/Spark SQL


#### Streaming Processing?
**Definition:**
Streaming in Spark (Structured Streaming) is designed to handle continuous data (Unbounded records), e.g., from Kafka, socket, files, or IoT sensors.

Instead of waiting for a full dataset, Spark processes incoming data in mini-batches or rows as they arrive.

**Example:**
Let’s say your app is receiving real-time transactions. You want to:

- Detect fraud in under 5 seconds
- Show live stats on a dashboard
- Alert if abnormal spending occurs

> Spark Structured Streaming lets you do this with the same DataFrame-style c
> Spark Module: SPark Structured Streaming

⚙️ **Spark's Unique Advantage**
Same API for Batch and Streaming
Spark Structured Streaming lets you write almost the same code for both batch and streaming. That makes it easier to build once and scale as needed.


### 🌐 **Supported Language APIs**

- **Scala and Java**: Native Spark APIs, first-class support.
- **PySpark**: Python API (Python wrapper over Spark Core and SQL modules via Py4J)
- **SparkR**: R API (R-language interface for Spark SQL and MLlib)

### 🚀 **Key Features of Spark**

- **In-Memory Processing:** Enables fast computations by reducing disk I/O.
- **Distributed Computing:** Tasks are executed in parallel across multiple cluster nodes.
- **Cluster Manager Support:**
    - **Standalone:** Spark’s built-in.
    - **YARN:** Widely used in Hadoop environments.
    - **Kubernetes:** Modern container orchestration.
    - **Mesos:** Legacy, less common today.
- **Fault Tolerance:** Achieved via lineage and DAG (Directed Acyclic Graph) recomputation.
- **Lazy Evaluation:** Computations are executed only upon triggering an action.
- **Catalyst Optimizer:** Optimizes SQL and DataFrame queries.
- **Tungsten Engine:** Enhances memory management and performance.
- **Structured Streaming:** Unified batch and real-time streaming API built on DataFrames.
- **Integration:** Compatible with NoSQL, SQL databases, and data lakes (MongoDB, MySQL, Delta Lake).
- **Multi-Language Support**: Scala, Java, Python, R.

### 🛠 **Learning and Usage Stages**

- **Learning & Prototyping:** PySpark with Jupyter Notebook.
- **Development:** PySpark within Docker for local clusters.
- **Production:** Kubernetes for scalable deployments.



---



## 📌 2. **Apache Spark Architecture Explained**
> Driver and Executors are processes that run on JVM.
Apache Spark follows a **driver-executor** architecture managed by a **cluster manager** (like YARN or Kubernetes). The main components are:

---

### **1. Driver**

The **Driver** is the **master coordinator** for a Spark application. It performs:

- Creating a **SparkSession** (which internally initializes components like `SparkContext`, `SQLContext`, etc.)
- Parsing user code into a **Logical Plan** and constructing a **DAG (Directed Acyclic Graph)** of stages.
- Requesting resources (executors) from the **Cluster Manager**.
- Scheduling and sending **tasks** to executors.
- Tracking task execution and handling **failures**.
- Collecting **results**, maintaining **metrics**, and managing **lineage**.

**🧠 Note:**

- Runs as a **single JVM process** on the **driver node**.
- Executes **your script** line by line, lazily building the execution plan until an **action** is called (`show()`, `collect()`, `write()`, etc.).

---

### **2. Executors**

**Executors** are **JVM processes** launched on worker nodes by the Cluster Manager.

They are responsible for:

- Reading their **own partition** of the data **directly from the source and not from any in-memory datasets stored in driver -** The **DataFrame object** *does not* hold actual data, it holds only the **logical plan** (blueprint). (e.g., HDFS, S3, JDBC).
- Executing the **tasks** assigned by the Driver.
- Performing **transformations** and **actions**.
- Caching intermediate results **in memory or disk**, if `.cache()` or `.persist()` is used.
- Writing output to sinks or returning results to the driver.

📌 Executors **do not share memory**. Each executor is independent and operates on **its own task/data partition**.


### **3. Cluster Manager**

The **Cluster Manager** allocates resources and starts up executors (and sometimes the driver). Spark can run on various cluster managers:

- **Standalone** – Spark’s built-in manager.
- **Apache YARN** – common in Hadoop ecosystems.
- **Kubernetes** – modern, container-based environments.
- **Apache Mesos** – legacy support.

> **Local** (Not a Cluster Manager)- Everything runs on your local machine (for learning/dev); In Local Mode, a Single JVM is used for driver and executors, and it is not managed by a cluster managers, spark handles this.

> **Execution Mode:** This decides where Spark runs and who manages the resources. Execution mode can be K8S, which means K8S is responsible for managing resources.

The cluster manager:
- **Allocates CPU and memory** for the Driver and Executors.
- **Launches** them as needed.
- **Manages** their lifecycle (restart, kill, stop, ).

> **Note:** 

#### 🧩 **Role of the Driver and Executors**

| Component | Runs on | Responsibility |
| --- | --- | --- |
| **Driver** | Client Node | Manages SparkContext, planning, scheduling, communication with cluster manager. |
| **Executor** | Worker Node | Executes tasks, stores data partitions, communicates results back to the Driver. |



---



## 🔄 3. **Lifecycle of a Spark Job**

1. **Application Submission:**
    - You submit your code via `spark-submit`.
2. **Driver Initialization:**
    - The driver registers with the cluster manager.
    - It instantiates a `SparkSession` (which embeds `SparkContext`, `SQLContext`).
    - Builds a **logical plan** based on your transformations. (lives in Driver memory)
3. **Resource Allocation:**
    - Driver requests resources (executors) from the cluster manager.
4. **Executor Launch:**
    - Executors start and **register back** with the driver.
5. **Task Scheduling & Distribution:**
    - Driver builds a DAG → splits into **stages** → splits stages into **tasks**.
    - Sends **task code (not data)** to executors. (
6. **Task Execution:**
    - Executors read their partition of the data from source.
    - Apply transformations and hold intermediate results in memory/disk.
    - Write final results to storage or return them to the Driver.
7. **Completion:**
    - Once all tasks finish, driver shuts down executors.



---



## 4 📂 **Core Spark Abstractions**

### **1. RDD (Resilient Distributed Dataset)**

- Immutable, distributed collection of data partitioned across nodes.
- Created via SparkContext; core abstraction for fault-tolerant processing.
- Low-level, manual optimization, no inherent schema.
- It is the **core abstraction (Data Structure)** in Spark for fault-tolerant, distributed data processing.
- You can find details of any application using RDD in Job and Stages section in the UI but not in SQL/Dataframe section.

### **2. DataFrame**

A Distributed collection of data structured into named columns (similar to SQL tables). (Conceptual definition).
- Built on top of RDD; utilizes Spark SQL.
- Created from `SparkSession.read()`.
- Optimized automatically by Catalyst Optimizer and Tungsten Engine.
- You can find details of any application using DF/DS in Job, Stages, Spark SQL/Dataframe section in the UI.
- a Spark **DataFrame** is not materialized when you define it. It's just a **logical plan** until you run an **action**.
> The definition is **conceptually** describing what a DataFrame **represents**, not what it *holds right now* in memory (What it holds in memory is logical plan, until during execution it becomes materialized - read into a row-column structure).
> DataFrame is generated by the driver after parsing the code. It is simply a logical plan that describes what data to read, how it is read and how data is presented.
> Materialized = read from source into memory in **partitions** across **executors**.
> **Partition** = a chunk of the data (a subset of rows). Spark tries to read these **in parallel**.
> **Task** = a unit of work that **processes one partition**. For example, a task might say: "Read partition X, filter it, join it, write result Y."
> 🔁 One **task per partition**.
>❗You **don’t send the partition** to the executor — the **task** tells the executor to go read its **own partition** from the **source** (HDFS, S3, DB, etc.)
>❗The data is not in the DataFrame. The **plan** is in the DataFrame. The **data** comes only when **executors** pull it during **task** execution.


### **3. Dataset**
Combines the benefits of RDD and Dataframe, offering a strongly-typed object oriented API with optimization capabilities of dataframes.  Created from SparkSession (Spark SQL Module)

- Strongly-typed distributed collection.
- Combines RDD (type-safety) and DataFrame (optimizations).
- Only supported natively in Scala and Java.

#### RDD vs. DataFrame/Dataset

| Aspect | RDD | DataFrame / Dataset |
| --- | --- | --- |
| Abstraction | Resilient Distributed Dataset: a low-level, immutable collection of records partitioned across the cluster.
 | Higher-level, tabular (rows & named columns), with an enforced schema (DataFrame) and optional compile-time type safety (Dataset in Scala/Java). |
| API style | Functional transforms (`map`, `filter`, `reduceByKey`, etc.). | Declarative or SQL-style (`select`, `filter`, `groupBy`, and SQL queries via `spark.sql`). |
| Optimization | Manual: you control shuffles, partitions; no automatic query planning. | Automatic: Catalyst optimizer rewrites and tunes your queries (predicate pushdown, projection pruning, join ordering, etc.). |
| Use cases | When you need full control (custom partitioning, complex loops) or before Structured APIs existed. | Most analytics, ETL, and ML use cases—easier, safer, and often faster. |

### What “Distributed” Means

- **Partitioned Data**
    - Both RDDs and DataFrames are split into **partitions**. A partition is simply a chunk of your dataset (e.g., a slice of an array or subset of rows).
    - Partitions are distributed across multiple **Executor** processes running on different nodes (or containers).
- **Parallel Execution**
    - The **Driver** (your program’s entry point, `SparkSession`) constructs a logical plan (DAG) of transformations.
    - That plan is divided into **tasks**, one task per partition (or per stage), and sent to Executors.
    - Executors run tasks in parallel, so if you have 100 partitions and 4 executors each with 2 cores, up to 8 tasks execute simultaneously.
- **Fault Tolerance**
    - If an Executor fails, the Driver reassigns its tasks on the remaining Executors using the lineage information (for RDDs) or query plan (for DataFrames).



---



## 🧱 5. Spark Contexts (SparkSession, SparkContext, SQLContext, HiveContext)

- `SparkSession` is the **new unified API entry point** from Spark 2.0 onward.
- It wraps:
    - **`SparkContext`** (for RDDs)
    - **`SQLContext`** (for DataFrames and SQL)
    - **`HiveContext`** (for Hive integration)

> So while SparkSession gives direct access to DataFrames/Datasets, it also gives indirect access to RDDs through its internal SparkContext.
> 

1. **Spark 2.0+ Unification**
    - Introduces **`SparkSession`** in the Spark SQL module
    - Behind the scenes, it creates and manages:
        - A `SparkContext` (for Core)
        - A `SQLContext` (for SQL)
        - A `HiveContext` if you enable Hive support

```python
spark = SparkSession.builder.appName("AppName").getOrCreate()

# You can still grab the SparkContext if you need it:
sc = spark.sparkContext
```

Approximately 90% of your work with Spark would be done using a SparkSession.

#### **Relationship: Spark, Spark Core, Spark Context, RDD**

- `Spark` = umbrella framework.
- `Spark Core` = engine with execution and RDD API.
- `SparkContext` = the old main entry point to Spark (for RDDs) defined by SparkCore
- `SparkSession` = the new unified entry point (for DataFrame, SQL, etc.).

### Data Loading diff. in SparkSession and SparkContext:

- `SparkContext.textFile(...)` creates **RDD**
- `SparkSession.read.csv(...)` creates **DataFrame** (backed by RDD internally)



---



## 🚦6. **Deployment Modes**
`Where the driver lives after submitting job`
- **Local Mode:** The driver and executors both live on your machine (so technically, it's also deployed locally). When `local[...]` is used. No cluster manager needed.
- **Client Mode:** Driver runs locally or on the machine which run thee submit command; executors run on the cluster. Ideal for testing/debugging.
- **Cluster Mode:** Driver runs within the cluster. Suitable for production.

### 📅 Deployment Mode Table

| Setup Type | Cluster Manager | Notes | `spark-submit` Snippet |
| --- | --- | --- | --- |
| **Local** | None | No cluster Man.| `--master local[*]` |
| **Standalone** | Spark Master | Simple Spark cluster | `--master spark://host:7077` |
| **YARN** | Hadoop YARN | Big data | `--master yarn --deploy-mode ...` |
| **Kubernetes** | K8s API | Cloud-native | `--master k8s://...` |
| **Mesos** | Mesos | Legacy | `--master mesos://...` |



---



## 📈 7. **Monitoring and Debugging**

- **Web UI:** Real-time job monitoring.
- **History Server:** Post-execution log analysis.



## 🏠 8. Best Practices for PySpark in Production

- Do **NOT** set `.master("local[*]")` in production code
**Do this For Production**

```python
spark = SparkSession.builder.appName("MyApp").getOrCreate()

# *When you write PySpark code using spark.read… or df.select… or spark.sql(…), you’re using that SQL layer—your code gets optimized, and you benefit from faster predicate pushdown, vectorized I/O, and Catalyst-driven planning, while still running on the same Core engine under the covers.*
```

**Use local[*] During testing or Development**: local[*], local[4]; where 4 is the Number of CPU core you want sprak to utilize.
- Apply cluster configs via `spark-submit` if you want to override Default Spark Configuration. 
**Example Spark-Submit for Kubernetes:**

```bash
spark-submit \
  --master k8s://https://<api-server> \
  --deploy-mode cluster \
  --conf spark.executor.instances=3 \
  --conf spark.kubernetes.container.image=my-image \
  app.py
```