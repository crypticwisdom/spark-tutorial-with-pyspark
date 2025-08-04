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