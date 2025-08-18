### Lab: Build SQLX with Advanced JS in Dataform

## Objective
Create a dynamic customer segmentation model. Segmentation rules live in a separate JavaScript config so you can update them without changing core SQL.

## Prerequisites
- Dataform repo connected to BigQuery
- Default project/dataset set in `workflow_settings.yaml`
- Sample data created (one-time)

Run to generate sample data:
```bash
dataform compile
dataform run --tags template_prerequisites
```

Files used in this lab:
- Source declaration: `definitions/models/core/1_bronze/dl_example_for_dataform.sqlx`
- Customer summary: `definitions/models/core/2_silver/customer_summary.sqlx`
- Segmentation model: `definitions/models/core/3_gold/customer_segmentation.sqlx`
- JS includes: `includes/segment_config.js`, `includes/customer_score.js`, `includes/agg_config.js`

---

## Step 1: Preamble — Data and Source Setup

- The raw table is declared in bronze (already provided):
  - `definitions/models/core/1_bronze/dl_example_for_dataform.sqlx` declares a table named `dl_example_generated_dataform`.
- One-time data generator:
  - `definitions/template_prerequisites/pregenerate_data.sqlx` creates a small nested dataset with orders per customer.

Verify the source exists (in BigQuery) after running the prerequisites.

---

## Step 2: Basic JavaScript Integration

Keep business rules in JS so BAs can edit logic without touching SQL.

- Segmentation rules in `includes/segment_config.js`:
```javascript
const segments = {
  "High Value": "lifetime_spend > 1000 AND order_count >= 5",
  "Frequent Shopper": "order_count > 5 AND lifetime_spend <= 1000",
  "New Customer": "order_count = 1",
  "At Risk": "days_since_last_order > 90 AND days_since_last_order < 365",
  "Churned": "days_since_last_order >= 365"
};

function build_segment_case_statement() {
  let caseStatement = "CASE\n";
  for (const segmentName in segments) {
    caseStatement += `    WHEN ${segments[segmentName]} THEN '${segmentName}'\n`;
  }
  caseStatement += "    ELSE 'General'\n  END";
  return caseStatement;
}

module.exports = { build_segment_case_statement };
```

- Customer score in `includes/customer_score.js`:
```javascript
function customer_score_expr() {
  return `
    0.5 * LEAST(lifetime_spend / 1000, 1) +
    0.3 * LEAST(order_count / 10, 1) +
    0.2 * (CASE WHEN days_since_last_order < 90 THEN 1 ELSE 0 END)
  `;
}
module.exports = { customer_score_expr };
```

Tip: To change segments, edit `includes/segment_config.js` and re-run the gold model.

---

## Step 3: Create the Customer Summary Table (Silver)

File: `definitions/models/core/2_silver/customer_summary.sqlx`

What it does:
- Flattens nested orders
- Computes core metrics: `order_count`, `lifetime_spend`, `last_order_date`, `days_since_last_order`
- Injects dynamic aggregations from `includes/agg_config.js`

Key snippet:
```sql
SELECT
  customer_id,
  ANY_VALUE(customer_name) AS customer_name,
  COUNT(*) AS order_count,
  SUM(order_amount) AS lifetime_spend,
  ARRAY_AGG(STRUCT(order_date, order_amount, categories) ORDER BY order_date DESC) AS order_history,
  ARRAY_AGG(DISTINCT category) AS all_categories,
  MAX(order_date) AS last_order_date,
  DATE_DIFF(CURRENT_DATE(), MAX(order_date), DAY) AS days_since_last_order,
  ${agg_config.build_agg_selects()}  -- dynamic aggregations (e.g., SUM/AVG/MAX/MEDIAN)
FROM
  orders_with_counts,
  UNNEST(categories) AS category
GROUP BY
  customer_id
```

---

## Step 4: Build the Dynamic Segmentation Table (Gold)

File: `definitions/models/core/3_gold/customer_segmentation.sqlx`

What it does:
- Reads from `customer_summary`
- Injects CASE logic from `segment_config.js`
- Adds `customer_score` from `customer_score.js`

Key snippet:
```sql
SELECT
  customer_id,
  customer_name,
  order_count,
  lifetime_spend,
  last_order_date,
  days_since_last_order,
  ${segment_config.build_segment_case_statement()} AS customer_segment,
  ${customer_score.customer_score_expr()} AS customer_score
FROM
  ${ref("customer_summary")}
```


---

## Step 5: End-to-End Run

- Silver then gold by tag:
```bash
dataform run --tags silver
dataform run --tags gold
```

- Or run everything:
```bash
dataform run
```

Validate in BigQuery:
```sql
-- Segments and scores
SELECT * FROM `...gold.customer_segmentation` ORDER BY customer_score DESC;

-- Summary metrics
SELECT * FROM `...silver.customer_summary` LIMIT 100;
```

Replace the project/dataset path with your environment or use the Dataform UI to explore the tables.

---

## Quick Exercises

- Update segmentation:
  - Add a rule in `includes/segment_config.js`, e.g.:
    - "VIP": "lifetime_spend > 3000 OR order_count > 20"
  - Re-run only the gold layer:
    ```bash
    dataform run --tags gold
    ```

- Adjust metrics:
  - Edit `includes/agg_config.js` to add/remove aggregations
  - Re-run silver, then gold:
    ```bash
    dataform run --tags silver
    dataform run --tags gold
    ```

- Tweak scoring weights:
  - Edit `includes/customer_score.js` weights
  - Re-run gold:
    ```bash
    dataform run --tags gold
    ```

---

## What you achieved
- SQLX + JS pattern: keep SQL stable, move business rules to JS
- Dynamic CASE for segmentation
- Reusable scoring and aggregation logic
