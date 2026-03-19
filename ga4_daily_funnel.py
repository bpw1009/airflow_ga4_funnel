"""
GA4 Daily Funnel Pipeline
=========================
Extracts daily GA4 e-commerce events, transforms into funnel metrics,
validates data quality, and loads to a dashboard-ready table.

Maps the current date to 2021 equivalent (T-1) to simulate a production
daily pipeline against the GA4 public sample dataset.
"""

from datetime import datetime, date, timedelta
from airflow.sdk import DAG, task

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook


# ── Config ───────────────────────────────────────────────────────────────
PROJECT_ID = "product-project-463105"
DATASET = "ga4_pipeline"  # you'll need to create this dataset in BQ
SOURCE_TABLE = "bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*"
STAGING_TABLE = f"{PROJECT_ID}.{DATASET}.stg_daily_events"
FUNNEL_TABLE = f"{PROJECT_ID}.{DATASET}.fct_daily_funnel"


DATASET_START = date(2020, 11, 1)
DATASET_END = date(2021, 1, 31)
DATASET_DAYS = (DATASET_END - DATASET_START).days + 1  # 92 days


def get_mapped_date() -> str:
    """Map today's date (T-1) to a date within the GA4 sample dataset range.

    The dataset covers 2020-11-01 to 2021-01-31 (92 days). We cycle through
    this range using modular arithmetic so every calendar day maps to a valid
    date in the dataset."""
    today = date.today()
    # Use a fixed reference point so the cycle is deterministic
    days_since_ref = (today - timedelta(days=1) - DATASET_START).days
    offset = days_since_ref % DATASET_DAYS
    query_date = DATASET_START + timedelta(days=offset)
    return query_date.strftime("%Y%m%d")


# ── DAG Definition ───────────────────────────────────────────────────────
with DAG(
    dag_id="ga4_daily_funnel",
    description="Daily GA4 funnel metrics pipeline mapped to 2021 sample data",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["ga4", "funnel", "bigquery"],
) as dag:

    # ── Task 1: Extract ──────────────────────────────────────────────────
    # Pull raw events for the mapped T-1 date into a staging table
    extract = BigQueryInsertJobOperator(
        task_id="extract_daily_events",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{STAGING_TABLE}` AS
                    SELECT
                        event_date,
                        event_name,
                        user_pseudo_id,
                        traffic_source.source AS traffic_source,
                        traffic_source.medium AS traffic_medium,
                        ecommerce.purchase_revenue AS purchase_revenue
                    FROM
                        `{SOURCE_TABLE}`
                    WHERE
                        _TABLE_SUFFIX = '{get_mapped_date()}'
                        AND event_name IN (
                            'session_start',
                            'view_item',
                            'add_to_cart',
                            'begin_checkout',
                            'purchase'
                        )
                """,
                "useLegacySql": False,
            }
        },
        project_id=PROJECT_ID,
        location="US",
    )

    # ── Task 2: Transform ────────────────────────────────────────────────
    # Aggregate into funnel metrics by channel
    transform = BigQueryInsertJobOperator(
        task_id="transform_funnel_metrics",
        configuration={
            "query": {
                "query": f"""
                    CREATE OR REPLACE TABLE `{FUNNEL_TABLE}` AS
                    WITH funnel_wide AS (
                        SELECT
                            event_date AS snapshot_date,
                            CONCAT(
                                COALESCE(traffic_source, '(none)'),
                                ' / ',
                                COALESCE(traffic_medium, '(none)')
                            ) AS channel,
                            COUNT(DISTINCT CASE WHEN event_name = 'session_start'
                                THEN user_pseudo_id END) AS sessions,
                            COUNT(DISTINCT CASE WHEN event_name = 'view_item'
                                THEN user_pseudo_id END) AS product_views,
                            COUNT(DISTINCT CASE WHEN event_name = 'add_to_cart'
                                THEN user_pseudo_id END) AS add_to_cart,
                            COUNT(DISTINCT CASE WHEN event_name = 'begin_checkout'
                                THEN user_pseudo_id END) AS begin_checkout,
                            COUNT(DISTINCT CASE WHEN event_name = 'purchase'
                                THEN user_pseudo_id END) AS purchases
                        FROM `{STAGING_TABLE}`
                        GROUP BY 1, 2
                    )
                    SELECT
                        snapshot_date,
                        channel,
                        stage_order,
                        funnel_stage,
                        users,
                        SAFE_DIVIDE(users,
                            FIRST_VALUE(users) OVER (
                                PARTITION BY snapshot_date, channel
                                ORDER BY stage_order
                            )
                        ) AS pct_of_sessions
                    FROM (
                        SELECT snapshot_date, channel, 1 AS stage_order,
                            'Sessions' AS funnel_stage, sessions AS users
                        FROM funnel_wide
                        UNION ALL
                        SELECT snapshot_date, channel, 2,
                            'Product Views', product_views
                        FROM funnel_wide
                        UNION ALL
                        SELECT snapshot_date, channel, 3,
                            'Add to Cart', add_to_cart
                        FROM funnel_wide
                        UNION ALL
                        SELECT snapshot_date, channel, 4,
                            'Begin Checkout', begin_checkout
                        FROM funnel_wide
                        UNION ALL
                        SELECT snapshot_date, channel, 5,
                            'Purchase', purchases
                        FROM funnel_wide
                    )
                    ORDER BY snapshot_date, channel, stage_order
                """,
                "useLegacySql": False,
            }
        },
        project_id=PROJECT_ID,
        location="US",
    )

    # ── Task 3: Data Quality Check ───────────────────────────────────────
    @task(task_id="check_data_quality")
    def check_data_quality():
        """Validate the transformed data before marking pipeline complete."""
        hook = BigQueryHook(project_id=PROJECT_ID, location="US")
        client = hook.get_client()

        # Check 1: rows exist
        row_count = list(client.query(
            f"SELECT COUNT(*) as cnt FROM `{FUNNEL_TABLE}`"
        ).result())[0].cnt

        if row_count == 0:
            raise ValueError(
                f"Data quality check failed: {FUNNEL_TABLE} has 0 rows. "
                f"Mapped date: {get_mapped_date()}"
            )

        # Check 2: no null funnel stages
        null_stages = list(client.query(
            f"""SELECT COUNT(*) as cnt FROM `{FUNNEL_TABLE}`
                WHERE funnel_stage IS NULL OR users IS NULL"""
        ).result())[0].cnt

        if null_stages > 0:
            raise ValueError(
                f"Data quality check failed: {null_stages} rows with null "
                f"funnel_stage or users in {FUNNEL_TABLE}"
            )

        # Check 3: sessions >= purchases for each channel (sanity check)
        bad_funnels = list(client.query(
            f"""
            SELECT channel
            FROM `{FUNNEL_TABLE}`
            WHERE funnel_stage = 'Sessions'
              AND users < (
                  SELECT users FROM `{FUNNEL_TABLE}` f2
                  WHERE f2.channel = `{FUNNEL_TABLE}`.channel
                    AND f2.funnel_stage = 'Purchase'
                    AND f2.snapshot_date = `{FUNNEL_TABLE}`.snapshot_date
              )
            """
        ).result())

        if len(bad_funnels) > 0:
            channels = [r.channel for r in bad_funnels]
            raise ValueError(
                f"Data quality check failed: purchases > sessions for "
                f"channels: {channels}"
            )

        print(f"All quality checks passed. {row_count} rows in {FUNNEL_TABLE}.")

    quality_check = check_data_quality()

    # ── Task 4: Log completion ───────────────────────────────────────────
    @task(task_id="log_completion")
    def log_completion():
        """Final task — confirms pipeline completed successfully."""
        mapped = get_mapped_date()
        print(
            f"Pipeline complete. Funnel data for GA4 date {mapped} "
            f"loaded to {FUNNEL_TABLE}."
        )

    done = log_completion()

    # ── Dependencies ─────────────────────────────────────────────────────
    extract >> transform >> quality_check >> done
