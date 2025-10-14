#!/usr/bin/env python3
"""
Docker entrypoint for IceCube notice listener.
Runs in production mode using environment variables for configuration.
"""

from gwtm_cron import gwtm_listener

if __name__ == "__main__":
    listener = gwtm_listener.listener.Listener(
        listener_type="ICECUBE_NOTICE"
    )
    listener.run(write_to_s3=True, verbose=True, dry_run=False)
