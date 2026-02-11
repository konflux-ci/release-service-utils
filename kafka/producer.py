#!/usr/bin/env python3

import argparse
import json
import os
from pathlib import Path

from confluent_kafka import Producer


def main():
    parser = argparse.ArgumentParser(description="Produce messages to Kafka")
    parser.add_argument(
        "--json-file",
        type=Path,
        required=True,
        metavar="FILE",
        help="Path to a JSON file whose content will be sent as the message",
    )
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
    parser.add_argument(
        "--header",
        action="append",
        default=[],
        metavar="KEY=VALUE",
        help="Message header (can be repeated). Example: --header advisory_state=updated",
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
        "acks": "all",
        "retries": 5,
        "message.timeout.ms": 60000,  # max ms from produce() to delivery/failure (1 min)
    }

    # Create Producer instance
    producer = Producer(config)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print("ERROR: Message failed delivery: {}".format(err))
        else:
            payload = json.loads(msg.value().decode("utf-8"))
            name = payload.get("metadata", {}).get("name", "<no name>")
            print(
                "Produced event to topic {topic}: metadata.name = {name}".format(
                    topic=msg.topic(), name=name
                )
            )

    with open(args.json_file) as f:
        myjson = json.load(f)

    headers = []
    for h in args.header:
        if "=" not in h:
            raise SystemExit(f"Invalid --header {h!r}: expected KEY=VALUE")
        key, _, value = h.partition("=")
        headers.append((key.strip(), value.encode("utf-8")))

    producer.produce(
        topic=topic,
        value=json.dumps(myjson),
        headers=headers if headers else None,
        callback=delivery_callback,
    )

    # Trigger any outstanding delivery report callbacks.
    producer.poll(0)

    # Block until the messages are delivered.
    producer.flush()


if __name__ == "__main__":
    main()
