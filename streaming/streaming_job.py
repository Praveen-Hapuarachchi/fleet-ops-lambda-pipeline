"""
Speed layer: Spark Structured Streaming job.

Reads `trip_events` from Kafka and maintains two windowed aggregates:

  1. Zone-level live utilization (1-minute tumbling window)     -> live_fleet_metrics
  2. Vehicle-level earnings for the current simulated day        -> vehicle_daily_earnings
     (window size = SIMULATED_DAY_SECONDS, consumed by the
      Airflow batch layer's daily profitability join)

Both aggregates are written via foreachBatch (upsert on the window's primary
key), and each successful micro-batch updates pipeline_heartbeat so the
serving layer's /health endpoint can detect a stalled stream.
"""

import json
import logging
import os
import sys
import time

import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, window, count, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
SIMULATED_DAY_SECONDS = int(os.environ.get("SIMULATED_DAY_SECONDS", "300"))

DB_CONFIG = {
    "host": os.environ.get("PIPELINE_DB_HOST", "postgres-app"),
    "dbname": os.environ.get("PIPELINE_DB_NAME", "fleet"),
    "user": os.environ.get("PIPELINE_DB_USER", "fleet"),
    "password": os.environ.get("PIPELINE_DB_PASSWORD", "fleet"),
}


class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps(
            {
                "ts": self.formatTime(record),
                "level": record.levelname,
                "stage": "speed_layer",
                "msg": record.getMessage(),
            }
        )


logger = logging.getLogger("speed_layer")
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(JsonFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)

EVENT_SCHEMA = StructType(
    [
        StructField("trip_id", StringType()),
        StructField("driver_id", StringType()),
        StructField("vehicle_id", StringType()),
        StructField("lat", DoubleType()),
        StructField("lon", DoubleType()),
        StructField("speed", DoubleType()),
        StructField("status", StringType()),
        StructField("zone", StringType()),
        StructField("fare", DoubleType()),
        StructField("timestamp", TimestampType()),
    ]
)


def get_conn():
    return psycopg2.connect(**DB_CONFIG)


def update_heartbeat(stage: str) -> None:
    try:
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO pipeline_heartbeat (stage, last_seen_at, status)
                VALUES (%s, now(), 'ok')
                ON CONFLICT (stage) DO UPDATE
                SET last_seen_at = now(), status = 'ok'
                """,
                (stage,),
            )
    except Exception as exc:
        logger.error(f"heartbeat update failed: {exc}")


def write_zone_metrics(batch_df, batch_id: int) -> None:
    rows = batch_df.collect()
    if not rows:
        logger.info(f"zone_metrics batch={batch_id} rows=0")
        return
    try:
        with get_conn() as conn, conn.cursor() as cur:
            for r in rows:
                idle_ratio = (
                    r["idle_vehicles"] / r["active_vehicles"]
                    if r["active_vehicles"] and r["active_vehicles"] > 0
                    else 0.0
                )
                cur.execute(
                    """
                    INSERT INTO live_fleet_metrics
                        (window_start, window_end, zone, active_vehicles,
                         idle_vehicles, idle_ratio, trips_started, total_earnings)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (window_start, zone) DO UPDATE SET
                        window_end = EXCLUDED.window_end,
                        active_vehicles = EXCLUDED.active_vehicles,
                        idle_vehicles = EXCLUDED.idle_vehicles,
                        idle_ratio = EXCLUDED.idle_ratio,
                        trips_started = EXCLUDED.trips_started,
                        total_earnings = EXCLUDED.total_earnings,
                        computed_at = now()
                    """,
                    (
                        r["window"]["start"],
                        r["window"]["end"],
                        r["zone"],
                        r["active_vehicles"] or 0,
                        r["idle_vehicles"] or 0,
                        round(idle_ratio, 4),
                        r["event_count"] or 0,
                        r["total_earnings"] or 0,
                    ),
                )
        logger.info(f"zone_metrics batch={batch_id} rows={len(rows)} status=ok")
        update_heartbeat("speed_layer_zone_metrics")
    except Exception as exc:
        logger.error(f"zone_metrics batch={batch_id} write failed: {exc}")


def write_vehicle_earnings(batch_df, batch_id: int) -> None:
    rows = batch_df.collect()
    if not rows:
        logger.info(f"vehicle_earnings batch={batch_id} rows=0")
        return
    try:
        with get_conn() as conn, conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    """
                    INSERT INTO vehicle_daily_earnings
                        (window_start, window_end, vehicle_id, total_earnings, trip_count)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (window_start, vehicle_id) DO UPDATE SET
                        window_end = EXCLUDED.window_end,
                        total_earnings = EXCLUDED.total_earnings,
                        trip_count = EXCLUDED.trip_count,
                        updated_at = now()
                    """,
                    (
                        r["window"]["start"],
                        r["window"]["end"],
                        r["vehicle_id"],
                        r["total_earnings"] or 0,
                        r["trip_count"] or 0,
                    ),
                )
        logger.info(f"vehicle_earnings batch={batch_id} rows={len(rows)} status=ok")
        update_heartbeat("speed_layer_vehicle_earnings")
    except Exception as exc:
        logger.error(f"vehicle_earnings batch={batch_id} write failed: {exc}")


def wait_for_kafka_topic(max_attempts: int = 20, delay_seconds: int = 5) -> None:
    """Small startup guard: retry until Kafka is actually reachable, since
    Spark's own retry-on-connect behaviour is not always forgiving of a
    slow-starting broker in Compose."""
    from kafka import KafkaConsumer
    from kafka.errors import NoBrokersAvailable

    for attempt in range(1, max_attempts + 1):
        try:
            consumer = KafkaConsumer(bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS)
            consumer.close()
            logger.info("kafka reachable")
            return
        except NoBrokersAvailable:
            logger.info(f"kafka not ready, attempt {attempt}/{max_attempts}")
            time.sleep(delay_seconds)
    logger.error("kafka never became reachable; proceeding anyway")


def main() -> None:
    wait_for_kafka_topic()

    spark = SparkSession.builder.appName("FleetSpeedLayer").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", "trip_events")
        .option("startingOffsets", "earliest")
        .load()
    )

    events = raw.select(
        from_json(col("value").cast("string"), EVENT_SCHEMA).alias("data")
    ).select("data.*").withWatermark("timestamp", "2 minutes")

    zone_metrics = events.groupBy(
        window(col("timestamp"), "1 minute"), col("zone")
    ).agg(
        count("*").alias("event_count"),
        spark_sum((col("status") == "idle").cast("int")).alias("idle_vehicles"),
        spark_sum((col("status") != "idle").cast("int")).alias("active_vehicles"),
        spark_sum("fare").alias("total_earnings"),
    )

    vehicle_earnings = events.groupBy(
        window(col("timestamp"), f"{SIMULATED_DAY_SECONDS} seconds"), col("vehicle_id")
    ).agg(
        spark_sum("fare").alias("total_earnings"),
        count("*").alias("trip_count"),
    )

    q1 = (
        zone_metrics.writeStream.outputMode("update")
        .foreachBatch(write_zone_metrics)
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", "/tmp/checkpoints/zone_metrics")
        .start()
    )

    q2 = (
        vehicle_earnings.writeStream.outputMode("update")
        .foreachBatch(write_vehicle_earnings)
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", "/tmp/checkpoints/vehicle_earnings")
        .start()
    )

    logger.info("speed layer streaming queries started")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
