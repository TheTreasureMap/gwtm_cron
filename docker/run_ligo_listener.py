#!/usr/bin/env python3
"""
Docker entrypoint for LIGO alert listener.
Runs in production mode using environment variables for configuration.

Environment variables:
    DRY_RUN: Set to 'true' or '1' to run in dry-run mode (no API calls, no storage writes)
    WRITE_TO_STORAGE: Set to 'false' or '0' to disable storage writes (default: true)
    VERBOSE: Set to 'false' or '0' to disable verbose logging (default: true)
    KAFKA_OFFSET_RESET: Set to 'earliest' to replay buffered messages from past few days (default: 'latest')
    KAFKA_GROUP_ID: Optional group ID for persistent offset tracking

Note: Storage type (S3, Azure, Swift) is controlled by STORAGE_BUCKET_SOURCE env var.

Replay Example:
    To ingest historical events from the Kafka buffer (past ~3 days):
    export KAFKA_OFFSET_RESET=earliest
    docker compose up ligo-listener
"""

import os
from gwtm_cron import gwtm_listener


def str_to_bool(value: str, default: bool = False) -> bool:
    """Convert string environment variable to boolean"""
    if not value:
        return default
    return value.lower() in ('true', '1', 'yes', 'on')


if __name__ == "__main__":
    # Read configuration from environment
    dry_run = str_to_bool(os.environ.get('DRY_RUN', ''), default=False)
    write_to_storage = str_to_bool(os.environ.get('WRITE_TO_STORAGE', ''), default=True)
    verbose = str_to_bool(os.environ.get('VERBOSE', ''), default=True)

    listener = gwtm_listener.listener.Listener(
        listener_type="LIGO_ALERT"
    )
    listener.run(write_to_storage=write_to_storage, verbose=verbose, dry_run=dry_run)
