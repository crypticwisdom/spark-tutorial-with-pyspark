https://youtu.be/mA96gUESVZc?si=OJSMhiUb6DUCHP3N

## ✅ 1. **What is Spark Configuration?**
**Definition:**
Spark configuration refers to the **key-value settings** that control how Spark behaves at runtime. These can be applied when submitting jobs (`spark-submit`), in the code via `SparkConf`, or through default files like `spark-defaults.conf`.

### 🔧 Examples:
* `spark.executor.memory` – memory per executor (e.g., `4g`)
* `spark.executor.instances` – number of executors
* `spark.sql.shuffle.partitions` – number of partitions post-shuffle (default: 200)
* `spark.driver.memory` – memory for driver

### 🟨 Purpose:
To set up Spark's **environment** and **execution parameters** before a job runs.

---












## ✅ 2. **What is Spark Tuning?**

**Definition:**
Spark tuning is the **process of adjusting configurations** to achieve **better performance and resource efficiency** based on your workload and cluster setup.

### 🔍 It includes:
* **Executor tuning** (e.g., memory, cores per executor)
* **Parallelism tuning** (`spark.default.parallelism`)
* **Shuffle tuning** (e.g., using `reduceByKey` over `groupByKey`)
* **Skew mitigation** (handling data imbalance)

### 🎯 Goal:
To **fine-tune Spark’s execution** so that your jobs are faster, cheaper, and more reliable.

---