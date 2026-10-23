# FinOps & Pipeline Observability Queries — DAV Database Trends

This document provides production SQL queries for monitoring pipeline efficiency, execution throughput, and cloud costs across Bronze, Silver, and Gold layers using `ops.pipeline_execution_logs` and `gold.audit_pipeline_runs`.

---

## 1. Execution Duration & Row Throughput by Layer

Computes total runtime, row processing throughput (rows/sec), and batch performance across all pipeline layers.

```sql
SELECT
    layer,
    COUNT(*) AS total_runs,
    SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful_runs,
    SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_runs,
    ROUND(AVG(UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time)), 2) AS avg_duration_seconds,
    ROUND(SUM(UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time)), 2) AS total_duration_seconds,
    SUM(rows_inserted) AS total_rows_inserted,
    SUM(rows_updated) AS total_rows_updated,
    SUM(COALESCE(rows_aggregated, 0)) AS total_rows_aggregated,
    ROUND(
        (SUM(rows_inserted + rows_updated + COALESCE(rows_aggregated, 0))) / 
        NULLIF(SUM(UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time)), 0),
        2
    ) AS processing_throughput_rows_per_sec
FROM workspace.ops.pipeline_execution_logs
GROUP BY layer
ORDER BY 
    CASE layer 
        WHEN 'BRONZE' THEN 1 
        WHEN 'SILVER' THEN 2 
        WHEN 'GOLD'   THEN 3 
        ELSE 4 
    END;
```

---

## 2. Latest Batch Execution Summary

Inspects the most recent pipeline batch across all target tables.

```sql
SELECT
    log_id,
    layer,
    table_name,
    repo_full_name,
    status,
    ROUND(UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time), 2) AS duration_seconds,
    rows_inserted,
    rows_updated,
    rows_aggregated,
    parameter,
    error_message
FROM workspace.ops.pipeline_execution_logs
WHERE batch_id = (
    SELECT batch_id 
    FROM workspace.ops.pipeline_execution_logs 
    ORDER BY start_time DESC 
    LIMIT 1
)
ORDER BY start_time ASC;
```

---

## 3. Daily Quarantine Anomaly Detection

Monitors data quality rejections across Bronze and Silver by reason and repository.

```sql
SELECT
    quarantine_date,
    layer,
    entity,
    repo_full_name,
    rejection_reason,
    quarantine_count
FROM workspace.gold.audit_quarantine_summary
ORDER BY quarantine_date DESC, quarantine_count DESC;
```

---

## 4. Gold Layer Parity Reconciliation Audit

Verifies that row counts between Silver source tables and Gold target tables remain 100% balanced.

```sql
SELECT
    table_name,
    silver_source_rows,
    gold_target_rows,
    delta_rows,
    status,
    check_timestamp
FROM workspace.gold.audit_gold_reconciliation
ORDER BY check_timestamp DESC, table_name ASC;
```

---

## 5. Databricks Serverless Cost Extrapolation Model

Databricks Serverless Compute bills automatically per second based on **DBUs (Databricks Units)** consumed by the serverless SQL and notebook execution engine.

### Cost Formula
$$\text{Cost per Run (\USD)} = \left(\frac{\text{Duration in Seconds}}{3600}\right) \times \text{DBU Consumption Rate} \times \text{Unit Price per DBU}$$

### Reference Parameters (Standard Serverless Compute)
* **Average Serverless Compute Allocation:** 2.0 DBU/hour (autoscaled small serverless cluster).
* **Sample DBU Price:** \$0.15 per DBU (Enterprise Serverless tier estimate).
* **Effective Hourly Compute Cost:** $2.0 \times \$0.15 = \$0.30$ / hour ($0.0000833 / second).

### Extrapolation SQL Query
```sql
SELECT
    batch_id,
    MIN(start_time) AS batch_start,
    MAX(end_time) AS batch_end,
    ROUND(SUM(UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time)), 2) AS total_compute_seconds,
    SUM(rows_inserted + rows_updated + COALESCE(rows_aggregated, 0)) AS total_rows_processed,
    -- Cost extrapolation assuming 2.0 DBU/hr @ $0.15/DBU ($0.30/hr total)
    ROUND((SUM(UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time)) / 3600.0) * 2.0 * 0.15, 4) AS estimated_cost_usd,
    -- Cost per 1,000 records processed
    ROUND(
        ((SUM(UNIX_TIMESTAMP(end_time) - UNIX_TIMESTAMP(start_time)) / 3600.0) * 2.0 * 0.15) / 
        NULLIF(SUM(rows_inserted + rows_updated + COALESCE(rows_aggregated, 0)) / 1000.0, 0),
        6
    ) AS estimated_cost_per_1k_rows_usd
FROM workspace.ops.pipeline_execution_logs
GROUP BY batch_id
ORDER BY batch_start DESC
LIMIT 10;
```

### Typical Pipeline Cost Profile (Benchmark)
* **Nightly Incremental Run:** ~45 seconds total execution time across 10 repos $\approx \mathbf{\$0.0038}$ per run.
* **Monthly Operational Cost (Nightly Cadence):** $30 \times \$0.0038 \approx \mathbf{\$0.11}$ / month.
* **Full Historical Re-sync (54,000+ records):** ~180 seconds total execution time $\approx \mathbf{\$0.015}$ one-off cost.
