from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

SNOWFLAKE_CONN_ID = "snowflake_default"
RAW_TABLE = "AIRFLOW_SALES_RAW"
SUMMARY_TABLE = "AIRFLOW_SALES_SUMMARY"
QUERY_TIMEOUT_SECONDS = 300

# Fixed dummy data: (order_id, region, product, quantity, unit_price)
DUMMY_ORDERS = [
    (1, "NORTH", "Laptop", 2, 950.00),
    (2, "NORTH", "Phone", 5, 400.00),
    (3, "SOUTH", "Laptop", 1, 950.00),
    (4, "SOUTH", "Tablet", 4, 300.00),
    (5, "EAST", "Phone", 3, 400.00),
    (6, "EAST", "Laptop", 6, 950.00),
    (7, "WEST", "Tablet", 2, 300.00),
    (8, "WEST", "Phone", 7, 400.00),
    (9, "NORTH", "Tablet", 1, 300.00),
    (10, "EAST", "Tablet", 5, 300.00),
]

default_args = {"retries": 2, "retry_delay": timedelta(seconds=30)}


def _run(statements):
    """Run (sql, params) pairs on one connection (Snowflake autocommits each statement)."""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    conn = hook.get_conn()
    try:
        cur = conn.cursor()
        cur.execute(f"ALTER SESSION SET STATEMENT_TIMEOUT_IN_SECONDS = {QUERY_TIMEOUT_SECONDS}")
        for sql, params in statements:
            cur.execute(sql, params)
    finally:
        conn.close()


with DAG(
    dag_id="snowflake_sales",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
    tags=["snowflake", "example"],
) as dag:

    @task
    def create_table():
        _run(
            [
                (
                    f"""
                    CREATE TABLE IF NOT EXISTS {RAW_TABLE} (
                        order_id   INTEGER,
                        region     STRING,
                        product    STRING,
                        quantity   INTEGER,
                        unit_price NUMBER(10, 2),
                        loaded_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                    )
                    """,
                    None,
                )
            ]
        )
        print(f"Table {RAW_TABLE} ready")

    @task
    def insert_dummy_data():
        # TRUNCATE + INSERT keeps re-runs idempotent (no duplicate rows).
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        try:
            cur = conn.cursor()
            cur.execute(f"TRUNCATE TABLE {RAW_TABLE}")
            cur.executemany(
                f"INSERT INTO {RAW_TABLE} (order_id, region, product, quantity, unit_price) "
                "VALUES (%s, %s, %s, %s, %s)",
                DUMMY_ORDERS,
            )
        finally:
            conn.close()
        print(f"Inserted {len(DUMMY_ORDERS)} rows into {RAW_TABLE}")

    @task
    def aggregate():
        # Persisted as a table so the result is visible in Snowflake itself.
        hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
        conn = hook.get_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                f"""
                CREATE OR REPLACE TABLE {SUMMARY_TABLE} AS
                SELECT region,
                       COUNT(*)                      AS orders,
                       SUM(quantity)                 AS units_sold,
                       SUM(quantity * unit_price)    AS revenue,
                       AVG(quantity * unit_price)    AS avg_order_value
                FROM {RAW_TABLE}
                GROUP BY region
                """
            )
            cur.execute(f"SELECT * FROM {SUMMARY_TABLE} ORDER BY revenue DESC LIMIT 100")
            cols = [c[0] for c in cur.description]
            rows = cur.fetchall()
        finally:
            conn.close()
        print(" | ".join(cols))
        for row in rows:
            print(" | ".join(str(v) for v in row))
        return [dict(zip(cols, (str(v) for v in row))) for row in rows]

    create_table() >> insert_dummy_data() >> aggregate()
