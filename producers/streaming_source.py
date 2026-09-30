"""
Streaming source simulator (Use Case 1: Ride-Hailing Fleet Operations).

Emits one trip GPS/telemetry event per vehicle every EMIT_INTERVAL_SECONDS to
the Kafka topic `trip_events`. This is the "speed layer" input.

Event schema:
    trip_id, driver_id, vehicle_id, lat, lon, speed, status
    (idle | enroute | on_trip), zone, fare, timestamp

Partitioning: messages are keyed by vehicle_id so each vehicle's events stay
ordered within one partition while load spreads across partitions.

Observability: structured JSON logs (same shape as the streaming, batch and
serving stages): startup, Kafka connection retries, periodic emit summaries
(events / errors / totals) and shutdown. Delivery failures are counted via
producer callbacks, so a broken broker shows up as errors in the summary
instead of failing silently.

TODO (optional):
    - Replace the random values in make_event with a per-vehicle state machine
      (idle -> enroute -> on_trip -> idle) so status, speed and fare are
      mutually consistent.
"""

import json
import logging
import os
import random
import signal
import sys
import threading
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

STAGE = "streaming_source"

KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
TOPIC = "trip_events"
EMIT_INTERVAL_SECONDS = float(os.environ.get("EMIT_INTERVAL_SECONDS", "3"))
NUM_VEHICLES = int(os.environ.get("NUM_VEHICLES", "20"))
LOG_INTERVAL_SECONDS = float(os.environ.get("LOG_INTERVAL_SECONDS", "30"))
CONNECT_RETRIES = int(os.environ.get("KAFKA_CONNECT_RETRIES", "30"))
CONNECT_RETRY_DELAY_SECONDS = float(os.environ.get("KAFKA_CONNECT_RETRY_DELAY", "5"))
ZONES = ["north", "south", "east", "west", "central"]
STATUSES = ["idle", "enroute", "on_trip"]


# ---------------------------------------------------------------------------
# Structured logging (same JSON shape as the other pipeline stages)
# ---------------------------------------------------------------------------
class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(
            {
                "ts": self.formatTime(record),
                "level": record.levelname,
                "stage": STAGE,
                "msg": record.getMessage(),
            }
        )


def build_logger() -> logging.Logger:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logger = logging.getLogger(STAGE)
    logger.setLevel(logging.INFO)
    logger.handlers = [handler]
    logger.propagate = False
    return logger


log = build_logger()


# ---------------------------------------------------------------------------
# Delivery statistics (updated from the producer's I/O thread callbacks)
# ---------------------------------------------------------------------------
class Stats:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.total_ok = 0
        self.total_err = 0
        self._interval_ok = 0
        self._interval_err = 0

    def on_success(self, _metadata) -> None:
        with self._lock:
            self.total_ok += 1
            self._interval_ok += 1

    def on_error(self, exc: BaseException) -> None:
        with self._lock:
            self.total_err += 1
            self._interval_err += 1
        log.error(f"delivery failed error={exc!r}")

    def snapshot_and_reset_interval(self) -> tuple[int, int]:
        with self._lock:
            ok, err = self._interval_ok, self._interval_err
            self._interval_ok = 0
            self._interval_err = 0
            return ok, err


# ---------------------------------------------------------------------------
# Kafka producer + event generation
# ---------------------------------------------------------------------------
def build_producer() -> KafkaProducer:
    """Create the producer, retrying while the broker is still starting."""
    for attempt in range(1, CONNECT_RETRIES + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=5,
            )
            log.info(f"kafka connected bootstrap={KAFKA_BOOTSTRAP_SERVERS} attempt={attempt}")
            return producer
        except NoBrokersAvailable:
            log.warning(
                f"kafka not reachable bootstrap={KAFKA_BOOTSTRAP_SERVERS} "
                f"attempt={attempt}/{CONNECT_RETRIES} retry_in={CONNECT_RETRY_DELAY_SECONDS}s"
            )
            threading.Event().wait(CONNECT_RETRY_DELAY_SECONDS)
    log.error("kafka connection failed, giving up")
    raise SystemExit(1)


def make_event(vehicle_id: str) -> dict:
    return {
        "trip_id": str(uuid.uuid4()),
        "driver_id": f"driver_{vehicle_id[-3:]}",
        "vehicle_id": vehicle_id,
        "lat": round(random.uniform(6.8, 7.2), 6),
        "lon": round(random.uniform(79.8, 80.2), 6),
        "speed": round(random.uniform(0, 80), 1),
        "status": random.choice(STATUSES),
        "zone": random.choice(ZONES),
        "fare": round(random.uniform(2, 40), 2),
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


def log_summary(stats: Stats) -> None:
    ok, err = stats.snapshot_and_reset_interval()
    msg = (
        f"emit_summary events={ok} errors={err} "
        f"total_events={stats.total_ok} total_errors={stats.total_err} "
        f"status={'ok' if err == 0 else 'degraded'}"
    )
    if err == 0:
        log.info(msg)
    else:
        log.warning(msg)


def main() -> None:
    stop = threading.Event()

    def handle_signal(signum, _frame) -> None:
        log.info(f"received signal={signum}, shutting down")
        stop.set()

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    stats = Stats()
    producer = build_producer()
    vehicle_ids = [f"vehicle_{i:03d}" for i in range(NUM_VEHICLES)]
    log.info(
        f"started topic={TOPIC} vehicles={NUM_VEHICLES} "
        f"interval_s={EMIT_INTERVAL_SECONDS} summary_every_s={LOG_INTERVAL_SECONDS}"
    )

    elapsed_since_summary = 0.0

    while not stop.is_set():
        for vehicle_id in vehicle_ids:
            try:
                future = producer.send(TOPIC, key=vehicle_id, value=make_event(vehicle_id))
                future.add_callback(stats.on_success)
                future.add_errback(stats.on_error)
            except KafkaError as exc:
                stats.on_error(exc)
        try:
            producer.flush(timeout=10)
        except KafkaError as exc:
            log.error(f"flush failed error={exc!r}")

        stop.wait(EMIT_INTERVAL_SECONDS)
        elapsed_since_summary += EMIT_INTERVAL_SECONDS
        if elapsed_since_summary >= LOG_INTERVAL_SECONDS:
            log_summary(stats)
            elapsed_since_summary = 0.0

    try:
        producer.flush(timeout=10)
        producer.close(timeout=10)
    finally:
        log_summary(stats)
        log.info("stopped")


if __name__ == "__main__":
    main()