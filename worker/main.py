import os
import time
from datetime import datetime, timezone

INTERVAL_SECONDS = int(os.getenv("WORKER_INTERVAL_SECONDS", "60"))


if __name__ == "__main__":
    while True:
        now = datetime.now(timezone.utc).isoformat()
        print(f"[worker] heartbeat at {now} | interval={INTERVAL_SECONDS}s", flush=True)
        time.sleep(INTERVAL_SECONDS)