# 2. Apache Spark Architecture

“Architecture, Modules, Performance, and Configuration tuning.”

**Ecosystem (Modules):**

- SparkSQL
- Streaming
- MLlib
- GraphX

**Spark Flavours on Cloud:**

- databricks
- google cloud dataproc
- ibm watson studio
- amazon emr
- Microsoft Azure Databricks

**Components in the Architecture:**

**- Driver and Executors** are processes that runs on the JVM, usually driver and executor runs on separate machines for the sake of management and avoid conflicts.

It can happen that we have 2 executors in 1 machine but its not common.

**lastly, the Cluster Manager (Yarn, Mesos, K8s).**

Say we have a data we have written the application in (Python, Java, SQL, Scala, R), The application is submitted and it communicates with the Spark Session which is the only entry point for Spark, the driver parses the application and analyses what we want to do with the data and also it will figure out the most efficient way of doing it, it will then split it into tasks (task, data partition info, conf, distributed variables), which will be distributed to the executors (each exec. process these in parallel, multiple tasks as the number of slots: numbers of slots = total number of cpu cores).

Say we have 2 executors with 4 cores each, each executor is able to process 4 task in parallel, the driver sends tasks to the executor and it also sends information a bout which executor would process/analyze which part of the data. Then executor grab the data and exchange the information between each other (if needed) and then send the results to the driver or writes to somewhere else.

**Example:**

Our application communicates with the Driver through spark session, Driver takes that application and analyze what’s the optimized way of executing it and also analyze the data, how much of data is it, how much executor we have and most efficient way not only in executing the application but also in distributing that datasets to the executors.