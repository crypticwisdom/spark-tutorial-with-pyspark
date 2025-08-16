# 🛠️ Optimization Techniques include

What user can do to Optimize Spark:

- Use optimized format for storaging data, like parquet, delta, instead of CSV
- Avoid expensize operations like sort
- Minimize volume of data
- Cache/Persist dataframes
- Repartition/Coalsce
- Avoid UDFs
- Partition and/or index data
- Bucketing
- Optimize Cluster
- Filtering early, selecting only needed columns
- ...

## 💡 Also includes

* Understanding **Catalyst Optimizer** (Spark SQL's logical/physical plan optimizer)
* Using **WholeStageCodegen**, **Tungsten**, and **Broadcast joins**

## 🧩 Other Related Areas You Might Have Missed

### 4. **Cluster Resource Management**

* Understanding YARN/Kubernetes and how Spark requests resources
* Dynamic allocation of executors
* Monitoring and auto-scaling

### 5. **Data Skew Handling**

* Using techniques like salting or custom partitioners

### 6. **Monitoring & Debugging**

* Reading Spark UI
* Interpreting DAG, stages, and tasks
* Using logs and metrics for troubleshooting

---