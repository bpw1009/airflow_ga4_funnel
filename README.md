# GA4 Daily Funnel Pipeline

An Apache Airflow DAG that runs a daily ELT pipeline against Google's public GA4 e-commerce dataset in BigQuery. Extracts raw event data, transforms it into channel-level funnel metrics, validates data quality, and loads results to a dashboard-ready table.

Built as a working demonstration of production pipeline patterns: incremental daily loads, staging/mart table separation, automated quality gates, and structured task dependencies.

## Pipeline Architecture

```
extract_daily_events → transform_funnel_metrics → check_data_quality → log_completion
```

**Extract** — Pulls raw GA4 events (session starts, product views, add-to-cart, checkout, purchases) for a single day into a staging table in BigQuery. Filters to funnel-relevant events only.

**Transform** — Aggregates staged events into funnel metrics by traffic channel. Pivots from wide to long format with a `pct_of_sessions` calculation using `FIRST_VALUE` window function. Output includes `snapshot_date`, `channel`, `funnel_stage`, `stage_order`, `users`, and `pct_of_sessions`.

**Data Quality Check** — Three validations before the pipeline marks success:
- Row count > 0 (data exists for the target date)
- No null values in `funnel_stage` or `users`
- Sessions ≥ purchases per channel (funnel monotonicity sanity check)

If any check fails, the pipeline stops and surfaces the specific failure reason in the Airflow logs.

**Log Completion** — Confirms successful load with the mapped date and destination table. In a production setting, this task would trigger a Slack notification or email alert.

## Date Mapping

The GA4 public sample dataset covers **2020-11-01 through 2021-01-31** (92 days). The pipeline simulates a live daily feed by mapping the current calendar date to a date within this range using modular arithmetic:

```python
days_since_ref = (today - 1 day - dataset_start).days
offset = days_since_ref % 92
query_date = dataset_start + offset
```

This means the pipeline always queries a valid date, cycles deterministically through the dataset, and applies T-1 logic (yesterday's data, as you'd expect in production — the full day isn't available until the following day).

## Output Schema

The final `fct_daily_funnel` table:

| Column | Type | Description |
|--------|------|-------------|
| `snapshot_date` | STRING | GA4 event date (YYYYMMDD) |
| `channel` | STRING | Traffic source / medium (e.g., `google / organic`) |
| `stage_order` | INT | Funnel stage position (1–5) |
| `funnel_stage` | STRING | Stage name: Sessions, Product Views, Add to Cart, Begin Checkout, Purchase |
| `users` | INT | Count of distinct users at this stage |
| `pct_of_sessions` | FLOAT | Conversion rate relative to sessions for this channel |

This long format is designed to plug directly into a BI tool (Tableau, Looker, etc.) for funnel visualization with channel filtering.

## Tech Stack

- **Orchestration:** Apache Airflow 3.1.8
- **Compute/Storage:** Google BigQuery
- **Data Source:** `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
- **Operators:** `BigQueryInsertJobOperator` (extract, transform), `@task`-decorated Python functions (quality check, logging)
- **Auth:** Google Application Default Credentials via `gcloud auth application-default login`

## Setup

### Prerequisites
- Python 3.10+
- Apache Airflow 3.x with `apache-airflow-providers-google`
- A GCP project with BigQuery enabled
- `gcloud` CLI authenticated with Application Default Credentials

### Installation

```bash
# Create and activate environment
mamba create -n airflow python=3.12 -y
mamba activate airflow

# Set Airflow home
export AIRFLOW_HOME=~/airflow

# Install Airflow with constraints
AIRFLOW_VERSION=3.1.8
PYTHON_VERSION="$(python -c 'import sys; print(f"{sys.version_info.major}.{sys.version_info.minor}")')"
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

# Install Google provider
pip install apache-airflow-providers-google

# Authenticate with GCP
gcloud auth application-default login
gcloud config set project YOUR_PROJECT_ID
```

### BigQuery Setup

Create a dataset named `ga4_pipeline` in your GCP project (US multi-region to match the public dataset).

### Airflow Connection

In the Airflow UI (Admin → Connections), create a connection:
- **Connection Id:** `google_cloud_default`
- **Connection Type:** Google Cloud
- **Project Id:** your GCP project ID

### Deploy the DAG

```bash
mkdir -p ~/airflow/dags
cp ga4_daily_funnel.py ~/airflow/dags/
airflow standalone
```

The DAG will appear in the Airflow UI at `localhost:8080`. Trigger manually or let the daily schedule run.

## Production Considerations

This is a working toy pipeline against a public dataset. In a production environment, you would:

- **Replace `CREATE OR REPLACE` with `INSERT`** to accumulate daily snapshots over time rather than overwriting
- **Add partitioning** on `snapshot_date` for query performance on the destination table
- **Use Airflow Variables or environment configs** instead of hardcoded project IDs and dataset names
- **Add alerting** — replace the log_completion task with Slack/email/PagerDuty notifications on both success and failure
- **Parameterize with execution date** — use Airflow's built-in `{{ ds }}` template variable instead of `date.today()` for idempotent reruns and proper backfill support
- **Add retry logic** — configure `retries` and `retry_delay` on tasks for transient BigQuery failures
- **Expand the DAG** — parallelize extracts from multiple sources, add intermediate transformation layers, branch on quality check outcomes

The four-task linear chain here uses the same building blocks as a production pipeline with hundreds of tasks. The pattern scales: extract → transform → validate → load, with dependencies made explicit and observable in the Airflow graph view.

## Related

This pipeline feeds the same funnel metrics used in a [Tableau Public conversion funnel dashboard](https://public.tableau.com/views/) built from the same GA4 dataset. The dashboard visualizes channel-level funnel performance with stage-level conversion rates and channel comparison views.
