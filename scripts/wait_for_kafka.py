"""Polls a Kafka broker until it accepts admin connections, or exits non-zero after a timeout.

Used in CI to make sure the Kafka service container has finished starting
up before the test suite tries to connect, since Docker's own health-check
timing does not always line up with when the broker is actually ready to
serve client requests.
"""

import argparse
import sys
import time

from kafka import KafkaAdminClient
from kafka.errors import NoBrokersAvailable


def wait(bootstrap_servers: str, timeout_seconds: int) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
            admin.close()
            return True
        except NoBrokersAvailable:
            time.sleep(2)
    return False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--timeout-seconds", type=int, default=60)
    args = parser.parse_args()

    if wait(args.bootstrap_servers, args.timeout_seconds):
        print("Kafka broker is ready.")
        sys.exit(0)
    else:
        print(f"Kafka broker not reachable at {args.bootstrap_servers} after {args.timeout_seconds}s.")
        sys.exit(1)


if __name__ == "__main__":
    main()
