Resources used:
- EMIL KAMINSKI: https://www.youtube.com/watch?v=iXVIPQEGZ9Y&list=PL19TxVqoJEnSWDIkyI8va3njcLrVKDfc9&index=4 ...
- https://www.youtube.com/watch?v=KnUXztKueMU&list=PLWAuYt0wgRcLCtWzUxNg4BjnYlCZNEVth


# LEARNING PATH
## For learning, local development, and even small projects:

* `pip install pyspark`
  is all you need.

**This pip package:**

* **Includes the Spark engine and all needed jars** for Python use.
* **Provides all the Spark CLI tools** (`pyspark`, `spark-submit`, etc.).
* **Works seamlessly with Jupyter Notebook** for interactive learning.


---

## **What You Can Do Now**

1. **You already have PySpark.**
2. **Just install Jupyter Lab (recommended) or Jupyter Notebook:**

   ```sh
    pip install jupyterlab
   ```
3. **Start it with:**

   ```sh
   jupyter lab
   ```
4. **Create a new Python notebook and try:**

   ```python
   from pyspark.sql import SparkSession
   spark = SparkSession.builder.master("local[*]").appName("MyFirstSparkApp").getOrCreate()
   spark.range(10).show()
   ```

---

## **When Would You Download Spark From the Official Site?**

* If you want to run a **multi-node Spark cluster** (YARN, Mesos, K8s, or Standalone).
* If you want to use **Scala, Java, or R** with Spark (pip only provides Python).
* If you need **full control** over Spark configs/jars for production cluster setups.

**But for 95% of learning, exploration, and even most PySpark projects:**

> **`pip install pyspark` is perfect and the fastest way to get started.**

---

## **Summary Table**

| Use Case                  | What to Install                 |
| ------------------------- | ------------------------------- |
| Learning PySpark, Jupyter | pip install pyspark, jupyterlab |
| Local data processing     | pip install pyspark             |
| Big cluster/Scala/Java    | Download from spark.apache.org  |

---

- !pip install pyspark