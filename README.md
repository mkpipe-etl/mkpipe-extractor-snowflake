# mkpipe-extractor-snowflake

Snowflake extractor plugin for [MkPipe](https://github.com/mkpipe-etl/mkpipe). Reads Snowflake tables using the native Snowflake Spark connector (`spark-snowflake`), which transfers data via internal staging (S3/Azure/GCS) — significantly faster than JDBC for large datasets.

## Documentation

For more detailed documentation, please visit the [GitHub repository](https://github.com/mkpipe-etl/mkpipe).

## License

This project is licensed under the Apache 2.0 License - see the [LICENSE](LICENSE) file for details.

---

## Connection Configuration

```yaml
connections:
  snowflake_source:
    variant: snowflake
    host: myaccount.snowflakecomputing.com
    port: 443
    database: MY_DATABASE
    schema: MY_SCHEMA
    user: myuser
    password: mypassword
    warehouse: MY_WAREHOUSE
```

With RSA key pair authentication:

```yaml
connections:
  snowflake_source:
    variant: snowflake
    host: myaccount.snowflakecomputing.com
    port: 443
    database: MY_DATABASE
    schema: MY_SCHEMA
    user: myuser
    warehouse: MY_WAREHOUSE
    private_key_file: /path/to/rsa_key.p8
    private_key_file_pwd: mypassphrase   # omit if key is unencrypted
```

---

## Table Configuration

```yaml
pipelines:
  - name: snowflake_to_pg
    source: snowflake_source
    destination: pg_target
    tables:
      - name: MY_SCHEMA.EVENTS
        target_name: stg_events
        replication_method: full
        fetchsize: 100000
```

### Incremental Replication

```yaml
      - name: MY_SCHEMA.EVENTS
        target_name: stg_events
        replication_method: incremental
        iterate_column: UPDATED_AT
        iterate_column_type: datetime
        partitions_column: ID
        partitions_count: 8
        fetchsize: 50000
```

### Custom SQL

```yaml
      - name: MY_SCHEMA.EVENTS
        target_name: stg_events
        replication_method: full
        custom_query: "SELECT ID, USER_ID, EVENT_TYPE, CREATED_AT FROM MY_SCHEMA.EVENTS WHERE {query_filter}"
```

Use `{query_filter}` as a placeholder — it is replaced with the incremental `WHERE` clause on incremental runs, or `WHERE 1=1` on full runs.

---

## Read Parallelism

Snowflake extractor uses the native Snowflake Spark connector with Spark's partition support. For large tables, set `partitions_column` and `partitions_count` to read in parallel:

```yaml
      - name: MY_SCHEMA.EVENTS
        target_name: stg_events
        replication_method: incremental
        iterate_column: UPDATED_AT
        iterate_column_type: datetime
        partitions_column: ID       # numeric column to split on
        partitions_count: 8         # number of parallel Spark partitions
        fetchsize: 50000
```

### How it works

- Spark reads the min/max of `partitions_column` and divides the range into `partitions_count` equal slices
- Each slice is fetched by a separate Spark task via the Snowflake connector
- `fetchsize` controls how many rows are fetched per round-trip

### Performance Notes

- **Full replication:** partitioning is not applied (only works with `incremental`).
- **`partitions_column`** should be a numeric column with good distribution (e.g. surrogate key).
- **Snowflake Warehouse size** is the primary performance lever — a larger warehouse processes queries faster regardless of JDBC partitioning.
- Keep `fetchsize` high (50,000–200,000) to minimize round-trips; Snowflake handles large result sets efficiently.

---

## All Table Parameters

| Parameter | Type | Default | Description |
|---|---|---|---|
| `name` | string | required | Snowflake table name (include schema if needed) |
| `target_name` | string | required | Destination table name |
| `replication_method` | `full` / `incremental` | `full` | Replication strategy |
| `iterate_column` | string | — | Column used for incremental watermark |
| `iterate_column_type` | `int` / `datetime` | — | Type of `iterate_column` |
| `partitions_column` | string | same as `iterate_column` | Column to split Spark reads on |
| `partitions_count` | int | `10` | Number of parallel Spark partitions |
| `fetchsize` | int | `100000` | Rows per JDBC fetch |
| `custom_query` | string | — | Override SQL with `{query_filter}` placeholder |
| `custom_query_file` | string | — | Path to SQL file (relative to `sql/` dir) |
| `write_partitions` | int | — | Coalesce to N partitions before writing |
| `tags` | list | `[]` | Tags for selective pipeline execution |
| `pass_on_error` | bool | `false` | Skip table on error instead of failing |



