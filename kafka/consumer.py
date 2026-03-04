#!/usr/bin/env python3

import argparse
import datetime
import json
import os
from pathlib import Path

from confluent_kafka import Consumer


def main():
    parser = argparse.ArgumentParser(description="Consume messages from Kafka")
    parser.add_argument(
        "--bootstrap-servers-file",
        type=Path,
        required=True,
        metavar="FILE",
        help="Path to a file containing the Kafka bootstrap servers",
    )
    parser.add_argument(
        "--username-file",
        type=Path,
        required=True,
        metavar="FILE",
        help="Path to a file containing the SASL username",
    )
    parser.add_argument(
        "--password-file",
        type=Path,
        required=True,
        metavar="FILE",
        help="Path to a file containing the SASL password",
    )
    args = parser.parse_args()

    def read_file(path: Path, name: str) -> str:
        try:
            return path.read_text().strip()
        except OSError as e:
            raise SystemExit(f"Failed to read {name} file {path}: {e}") from e

    bootstrap_servers = read_file(args.bootstrap_servers_file, "bootstrap servers")
    username = read_file(args.username_file, "username")
    password = read_file(args.password_file, "password")
    topic = os.environ.get("KAFKA_TOPIC")
    if not topic:
        raise SystemExit("Set KAFKA_TOPIC env variable")

    config = {
        "bootstrap.servers": bootstrap_servers,
        "sasl.username": username,
        "sasl.password": password,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "SCRAM-SHA-512",
        "group.id": "kafka-python-getting-started",
        "auto.offset.reset": "earliest",
    }

    consumer = Consumer(config)
    consumer.subscribe([topic])

    # Poll for new messages from Kafka and print them.
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
            elif msg.error():
                print("ERROR: {}".format(msg.error()))
            else:
                seconds_since_epoch = msg.timestamp()[1] / 1000
                time_str = (
                    datetime.datetime.utcfromtimestamp(seconds_since_epoch)
                    .replace(microsecond=0)
                    .isoformat()
                )
                value_bytes = msg.value()
                value_str = value_bytes.decode("utf-8")
                try:
                    value_json = json.loads(value_str)
                    value_pretty = json.dumps(value_json, indent=2)
                except json.JSONDecodeError:
                    value_pretty = value_str
                headers = msg.headers() or []
                headers_dict = {
                    k: v.decode("utf-8") if isinstance(v, bytes) else v for k, v in headers
                }
                headers_pretty = json.dumps(headers_dict, indent=2) if headers_dict else "{}"
                print(
                    "--- Consumed event from topic {} at {} ---".format(msg.topic(), time_str)
                )
                print("Headers:\n{}".format(headers_pretty))
                print("Message:\n{}".format(value_pretty))
                print("---")
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()


if __name__ == "__main__":
    main()
