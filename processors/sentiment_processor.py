import os
import sys
from pyflink.table import EnvironmentSettings, StreamTableEnvironment

def run_sentiment_processor(event_time: bool = False):
    # 1. Initialize Flink Table Environment (Starts the JVM)
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(environment_settings=settings)

    # 2. Dependency Management (The "lib" folder approach)
    current_dir = os.path.dirname(os.path.realpath(__file__))
    # Points to sentinel-stream/lib/
    lib_dir = os.path.join(current_dir, "..", "lib")
    
    # We load the Kafka connector from your local lib
    kafka_jar = os.path.join(lib_dir, "flink-sql-connector-kafka.jar")
    
    if not os.path.exists(kafka_jar):
        raise FileNotFoundError(f"❌ JAR not found at {kafka_jar}. Run uv run --no-project --python .venv/bin/python scripts/setup_depts.py first!")

    t_env.get_config().set("pipeline.jars", f"file://{kafka_jar}")

    # 3. Define the Kafka Source Table
    # event_time=True: use row_time watermark (production-correct, requires live/latest records)
    # event_time=False: use proc_time (fast demo output, works with any offset)
    startup_mode = "latest-offset" if event_time else "earliest-offset"

    watermark_clause = (
        "WATERMARK FOR row_time AS row_time - INTERVAL '5' SECOND,"
        if event_time else ""
    )

    t_env.execute_sql(f"""
        CREATE TABLE kafka_reviews (
            brand_name STRING,
            user_tier STRING,
            sentiment_score DOUBLE,
            `timestamp` BIGINT,
            row_time AS TO_TIMESTAMP_LTZ(`timestamp`, 3),
            {watermark_clause}
            proc_time AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = 'raw-reviews',
            'properties.bootstrap.servers' = 'localhost:9092',
            'properties.group.id' = 'sentiment-analysis-group',
            'scan.startup.mode' = '{startup_mode}',
            'format' = 'json'
        )
    """)

    # 4. The Transformation (The 11/3 Weighted Average Logic)
    # HOP defines a Sliding Window (Size 20 Sec, Slide 5 Sec)
    time_col = "row_time" if event_time else "proc_time"

    result_table = t_env.sql_query(f"""
        SELECT
            brand_name,
            window_start,
            window_end,
            SUM(CASE WHEN user_tier = 'Premium' THEN sentiment_score * 2 ELSE sentiment_score END)
                / SUM(CASE WHEN user_tier = 'Premium' THEN 2 ELSE 1 END) AS weighted_avg_sentiment,
            COUNT(*) AS review_count
        FROM TABLE(
            HOP(
                TABLE kafka_reviews,
                DESCRIPTOR({time_col}),
                INTERVAL '5' SECONDS,
                INTERVAL '20' SECONDS
            )
        )
        GROUP BY
            brand_name,
            window_start,
            window_end
    """)

    mode_label = "EVENT-TIME (row_time watermark, latest-offset)" if event_time else "PROCESSING-TIME (proc_time, earliest-offset)"
    print(f"🚀 Flink Job Started [{mode_label}]. Windows will print every 5s...")
    
    # 5. The Sink (Printing to terminal for now)
    result_table.execute().print()

if __name__ == '__main__':
    event_time_mode = "--event-time" in sys.argv
    run_sentiment_processor(event_time=event_time_mode)