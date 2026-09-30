"""
Daily batch source simulator (Use Case 1: Ride-Hailing Fleet Operations).

Drops one CSV file per simulated day into ./airflow/data, representing the
fuel/maintenance expense feed from garages and fuel partners. Airflow's
daily DAG picks this file up on its schedule.

Schema: vehicle_id, fuel_cost, maintenance_cost, distance_covered, service_flag

Simulated clock: one "day" = SIMULATED_DAY_SECONDS (default 300s / 5 minutes).
The day index is derived as floor(unix_time / SIMULATED_DAY_SECONDS) rather
than an incrementing local counter, so it lines up with the streaming job's
tumbling-window boundary (Spark windows are epoch-aligned by default) without
any explicit coordination between the two containers. State this
compression clearly in the report per the assignment's submission
guidelines.

TODO (Days 5-7):
    - Decide file-arrival signalling for Airflow: a FileSensor watching
      this directory, or an API call this script makes to trigger the DAG.
"""

import csv
import json
import logging
import os
import random
import sys
import time
from datetime import datetime, timezone

OUTPUT_DIR = "/data"
NUM_VEHICLES = int(os.environ.get("NUM_VEHICLES", "20"))
SIMULATED_DAY_SECONDS = float(os.environ.get("SIMULATED_DAY_SECONDS", "300"))


class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps(
            {
                "ts": self.formatTime(record),
                "level": record.levelname,
                "stage": "batch_source",
                "msg": record.getMessage(),
            }
        )


logger = logging.getLogger("batch_source")
_handler = logging.StreamHandler(sys.stdout)
_handler.setFormatter(JsonFormatter())
logger.addHandler(_handler)
logger.setLevel(logging.INFO)


def current_day_bucket() -> int:
    return int(time.time() // SIMULATED_DAY_SECONDS)


def write_daily_file(day_index: int) -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filename = os.path.join(OUTPUT_DIR, f"vehicle_expenses_day{day_index:03d}.csv")
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["vehicle_id", "fuel_cost", "maintenance_cost", "distance_covered", "service_flag"]
        )
        for i in range(NUM_VEHICLES):
            vehicle_id = f"vehicle_{i:03d}"
            writer.writerow(
                [
                    vehicle_id,
                    round(random.uniform(10, 60), 2),
                    round(random.uniform(0, 100), 2),
                    round(random.uniform(50, 400), 1),
                    random.random() < 0.05,
                ]
            )
    return filename


def main() -> None:
    logger.info(f"starting; one simulated day = {SIMULATED_DAY_SECONDS}s")
    last_bucket = None
    while True:
        bucket = current_day_bucket()
        if bucket != last_bucket:
            path = write_daily_file(bucket)
            logger.info(f"wrote {path} for day_bucket={bucket}")
            last_bucket = bucket
        # check slightly more often than the day length so we don't
        # overshoot a bucket boundary by much
        time.sleep(max(1.0, SIMULATED_DAY_SECONDS / 10))


if __name__ == "__main__":
    main()
