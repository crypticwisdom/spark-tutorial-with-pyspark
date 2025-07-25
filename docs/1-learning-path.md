# **Recommended Learning Path (PySpark + Spark in Jupyter)**

### **1. Spark and PySpark Fundamentals**

- **What is Spark? What is PySpark?**
- **Spark’s Architecture:** Driver, Executors, Cluster Manager (you’ll start with “local” mode)
- **RDDs vs DataFrames vs Datasets** (DataFrames are the main focus for Python)

---

### **2. The Basics (with Examples in Jupyter)**

- **Starting a SparkSession**
- **Creating DataFrames from Python objects, CSV, and Parquet files**
- **Basic DataFrame Operations:**
    - `select`, `filter`, `where`
    - `withColumn`, `drop`, `distinct`
    - Sorting: `orderBy`, `sort`
- **Data Types and Schemas**

---

### **3. Intermediate Data Operations**

- **Aggregations and GroupBy**
- **Joins (inner, left, right, outer)**
- **Handling Missing Data (`dropna`, `fillna`)**
- **User-Defined Functions (UDFs)**

---

### **4. SQL with Spark**

- **Register Dataframes as temporary tables**
- **Run SQL queries inside Spark**

---

### **5. Data Ingestion and Output**

- **Reading/Writing CSV, JSON, and Parquet files**
- **Basic introduction to S3/minio storage or HDFS**

---

### **6. Advanced:**

- **Spark Streaming with PySpark**
- **Window functions**
- **Performance tuning: partitions, caching, broadcast joins**
- **Working with Big Data on real datasets**

---

## **Learning Strategy**

- **Follow along with official and community tutorials in your own notebook.**
- **Tweak the code and try new things after each example.**
- **Document what you learn with markdown cells** (“# This cell shows filtering”, etc.).
- Try your own ideas as you go!

---

Spark AQE, coalesce, repartitioning,

Resource I found:

https://sparkbyexamples.com/

[1. Guide to Apache Spark](https://www.notion.so/1-Guide-to-Apache-Spark-228001fbed0d80bca0f1e262db701313?pvs=21)

[2. Apache Spark Architecture](https://www.notion.so/2-Apache-Spark-Architecture-22a001fbed0d805784d2f49f9222354d?pvs=21)