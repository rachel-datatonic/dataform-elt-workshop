### Lab: Automate and monitor Dataform workflow with different deployment strategies


## Objectives
- Run models manually in dev
- Create a production schedule using schema suffix per developer to avoid collisions
- Export Dataform run logs to BigQuery (log sink)
- Create a deployment log table in your project and log each run
- Build a simple analysis view to understand run outcomes (success/failure, timestamp, id)

## Prerequisites
- Dataform repo connected to BigQuery
- Permissions to create a BigQuery dataset (for logs)
- Dataform actions compile successfully: `dataform compile`

---

## Part 1 — Manual runs in dev (schema suffix)
Schema suffix isolates datasets per developer (e.g., `_rachel`).

Tips
- Use short lowercase suffix, e.g., `_rachel`
- Keep suffix consistent across runs to avoid scattering tables

---

## Part 2 — Production schedule with per‑developer isolation
Create a schedule in the Dataform UI:
1. Releases & scheduling → Create Release configuration
2. Branch: `main` (or your prod branch)
3. Actions: choose tags (e.g., `gold`) or specific actions
4. Frequency: pick a time (e.g., daily 07:00)
5. Advanced → Schema suffix: `_<your_name>` to avoid collisions
6. Save

Result: Each developer’s schedule writes to isolated datasets.

---

## Part 3 — Export Dataform logs to BigQuery (Log Sink)

1) Create a BigQuery dataset for logs (e.g., `dataform_logs`).

2) In Google Cloud Console → Logging → Log Router → Create Sink:
- Name: `dataform_to_bq`
- Destination: BigQuery dataset → `dataform_logs`
- Use partitioned table (recommended)
- Sink filter (captures Dataform audit activity):
```
protoPayload.@type="type.googleapis.com/google.cloud.audit.AuditLog"
protoPayload.serviceName="dataform.googleapis.com"
```
- Grant writer permission to the sink service account
- Create

Logs will start populating after new runs. Common table: `dataform_logs.cloudaudit_googleapis_com_activity`.

---

## Part 4 — Build an analysis view (success/failure, timestamp, id)

Create `definitions/workflow/workflow_runs_analysis.sqlx`:
```sql
config {
  type: "view",
  schema: "workflow",
  tags: ["workflow", "monitoring"]
}

 SELECT
  timestamp,
  receiveTimestamp AS receive_timestamp,
  severity,
  jsonpayload_v1_workflowinvocationcompletionlogentry.terminalstate AS terminal_state,
  CASE
    WHEN jsonpayload_v1_workflowinvocationcompletionlogentry.terminalstate = 'FAILED'
    THEN textPayload
    ELSE NULL
  END AS error_message,
  jsonpayload_v1_workflowinvocationcompletionlogentry.workflowinvocationid AS invocation_id,
  resource.labels.repository_id
FROM
  
   ${ref("dataform_googleapis_com_workflow_invocation_completion")}
WHERE
  jsonpayload_v1_workflowinvocationcompletionlogentry.workflowinvocationid IS NOT NULL
```

Query it:
```sql
SELECT *
FROM `workflow.deployment_log`
ORDER BY run_timestamp DESC
LIMIT 100;
```

---

## End‑to‑end checklist
- Dev run with schema suffix
- Schedule with per‑developer isolation
- Log sink to BigQuery dataset
- Deployment log action exists and runs
- Analysis view returns SUCCESS/ERROR with timestamps and IDs
