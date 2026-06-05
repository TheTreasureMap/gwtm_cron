#!/usr/bin/env python3
"""
Selective Alert Ingestion Tool

This script helps you catch up on alerts selectively by:
1. Listing all available alerts in the Kafka stream (without ingesting)
2. Ingesting only alerts matching a specific filter string from Kafka
3. Downloading and ingesting specific alerts directly from GraceDB URLs

Usage:
    # List mode: Show all available alerts without ingestion
    python selective_ingest.py --list --max-alerts 50

    # Ingest mode: Process only alerts matching a filter string from Kafka
    python selective_ingest.py --ingest --filter "S230" --write --no-dry-run

    # GraceDB mode: Download specific alerts directly from GraceDB
    python selective_ingest.py --gracedb --ids S251010al S251011x --write --no-dry-run

    # Test mode: Show what would be ingested (dry run)
    python selective_ingest.py --ingest --filter "S230"
"""

import sys
import os
import json
import argparse
import requests
from datetime import datetime

# Add src to path
sys.path.insert(0, 'src/')

from gcn_kafka import Consumer
from gwtm_cron.gwtm_listener import listener, gw_config

def list_alerts(config_path, listener_type, max_alerts=50, timeout_seconds=60, debug=False):
    """
    List available alerts without ingesting them.

    Args:
        config_path: Path to listener config JSON
        listener_type: Type of listener (LIGO_ALERT or ICECUBE_NOTICE)
        max_alerts: Maximum number of alerts to list
        timeout_seconds: How long to wait for alerts
    """
    print("="*80)
    print(f"LISTING MODE - {listener_type}")
    print("="*80)
    print(f"Connecting to Kafka stream: {listener.LISTENER_TYPES[listener_type]['domain']}")
    print(f"Will list up to {max_alerts} alerts (timeout: {timeout_seconds}s)")
    print(f"Press Ctrl+C to stop early\n")

    config = gw_config.Config(path_to_config=config_path)

    # Build consumer config with offset reset
    consumer_config = {'broker.address.family': 'v4'}
    offset_reset = os.environ.get('KAFKA_OFFSET_RESET', 'latest')
    consumer_config['auto.offset.reset'] = offset_reset

    print(f"Kafka offset reset: {offset_reset}")

    consumer = Consumer(
        config=consumer_config,
        client_id=config.KAFKA_CLIENT_ID,
        client_secret=config.KAFKA_CLIENT_SECRET
    )

    consumer.subscribe([listener.LISTENER_TYPES[listener_type]["domain"]])

    alerts_found = []
    last_activity_time = datetime.now()

    try:
        while len(alerts_found) < max_alerts:
            # Timeout based on inactivity (no alerts received), not total runtime
            elapsed_since_activity = (datetime.now() - last_activity_time).total_seconds()
            if elapsed_since_activity > timeout_seconds:
                print(f"\nInactivity timeout reached ({timeout_seconds}s with no new alerts)")
                break

            for message in consumer.consume(timeout=1):
                # Reset activity timer when we receive an alert
                last_activity_time = datetime.now()
                try:
                    alert_data = json.loads(message.value())

                    # Debug: print raw JSON structure
                    if debug and len(alerts_found) < 3:
                        print(f"\n{'='*60}")
                        print("DEBUG: Raw alert data:")
                        print(json.dumps(alert_data, indent=2))
                        print(f"{'='*60}\n")

                    # Extract key fields based on listener type
                    if listener_type == "LIGO_ALERT":
                        alert_id = alert_data.get('superevent_id', 'UNKNOWN')
                        alert_type = alert_data.get('alert_type', 'UNKNOWN')
                        time_created = alert_data.get('time_created', 'UNKNOWN')
                        event_time = alert_data.get('event', {}).get('time', 'UNKNOWN')
                    elif listener_type == "ICECUBE_NOTICE":
                        # IceCube notices have different structure
                        alert_id = alert_data.get('ref_ID', 'UNKNOWN')
                        # Try to get LVK reference alert ID
                        if alert_id == 'UNKNOWN' and 'reference' in alert_data:
                            ref = alert_data.get('reference', {})
                            alert_id = ref.get('gcn.notices.LVK.alert', 'UNKNOWN')
                        alert_type = f"pval_gen={alert_data.get('pval_generic', 'N/A')}"
                        time_created = alert_data.get('alert_datetime', 'UNKNOWN')
                        event_time = time_created

                    alert_info = {
                        'id': alert_id,
                        'type': alert_type,
                        'created': time_created,
                        'event_time': event_time
                    }

                    alerts_found.append(alert_info)

                    # Print alert info
                    print(f"{len(alerts_found):3d}. {alert_id:20s} | {alert_type:15s} | {time_created}")

                    if len(alerts_found) >= max_alerts:
                        break

                except json.JSONDecodeError:
                    print(f"Warning: Could not parse alert as JSON")
                except Exception as e:
                    print(f"Warning: Error processing alert: {e}")

    except KeyboardInterrupt:
        print("\n\nStopped by user")

    print("\n" + "="*80)
    print(f"Found {len(alerts_found)} alerts")
    print("="*80)

    if alerts_found:
        print("\nUnique alert IDs:")
        unique_ids = sorted(set(a['id'] for a in alerts_found))
        for alert_id in unique_ids:
            count = sum(1 for a in alerts_found if a['id'] == alert_id)
            print(f"  {alert_id} (x{count})")

    return alerts_found


def ingest_filtered(config_path, listener_type, filter_string, write_to_s3=False,
                   dry_run=True, max_alerts=100, timeout_seconds=60, exclude_test=False):
    """
    Ingest only alerts matching the filter string.

    Args:
        config_path: Path to listener config JSON
        listener_type: Type of listener (LIGO_ALERT or ICECUBE_NOTICE)
        filter_string: Only ingest alerts containing this string in their ID
        write_to_s3: Whether to write to S3 storage
        dry_run: If True, don't post to API
        exclude_test: If True, exclude test alerts (MS alerts)
        max_alerts: Maximum number of alerts to process
        timeout_seconds: How long to wait for alerts
    """
    print("="*80)
    print(f"INGESTION MODE - {listener_type}")
    print("="*80)
    print(f"Filter: Only alerts containing '{filter_string}'")
    if exclude_test:
        print(f"Excluding test alerts (MS*)")
    print(f"Write to S3: {write_to_s3}")
    print(f"Dry run: {dry_run} (API posting {'DISABLED' if dry_run else 'ENABLED'})")
    print(f"Max alerts: {max_alerts} (timeout: {timeout_seconds}s)")
    print(f"Press Ctrl+C to stop\n")

    # Create listener
    l = listener.Listener(listener_type=listener_type, config_path=config_path)

    alerts_processed = 0
    alerts_skipped = 0
    alerts_examined = 0
    last_activity_time = datetime.now()

    try:
        while alerts_processed < max_alerts:
            # Timeout based on inactivity (no alerts received), not total runtime
            elapsed_since_activity = (datetime.now() - last_activity_time).total_seconds()
            if elapsed_since_activity > timeout_seconds:
                print(f"\nInactivity timeout reached ({timeout_seconds}s with no new alerts)")
                break

            for message in l.consumer.consume(timeout=1):
                # Reset activity timer when we receive an alert
                last_activity_time = datetime.now()
                alerts_examined += 1
                try:
                    alert_data = json.loads(message.value())

                    # Get alert ID based on listener type
                    if listener_type == "LIGO_ALERT":
                        alert_id = alert_data.get('superevent_id', '')
                        alert_type = alert_data.get('alert_type', 'UNKNOWN')
                    elif listener_type == "ICECUBE_NOTICE":
                        # IceCube notices have different structure
                        alert_id = alert_data.get('ref_ID', '')
                        # Try to get LVK reference alert ID
                        if not alert_id and 'reference' in alert_data:
                            ref = alert_data.get('reference', {})
                            alert_id = ref.get('gcn.notices.LVK.alert', '')
                        alert_type = f"pval_gen={alert_data.get('pval_generic', 'N/A')}"

                    # Check if alert should be excluded (test alerts)
                    is_test_alert = alert_id.startswith('MS')
                    if exclude_test and is_test_alert:
                        alerts_skipped += 1
                        continue

                    # Check if alert matches filter
                    if filter_string.lower() in alert_id.lower():
                        print(f"\n{'='*60}")
                        print(f"PROCESSING: {alert_id} ({alert_type})")
                        print(f"{'='*60}")

                        # Process the alert
                        alert, ext_alert = l._listen(
                            alert=message.value(),
                            write_to_storage=write_to_s3,
                            verbose=True,
                            dry_run=dry_run
                        )

                        print(f"\nResult:")
                        print(alert)
                        if ext_alert:
                            print("\nExternal Alert:")
                            print(ext_alert)

                        alerts_processed += 1
                        print(f"\n✓ Processed {alerts_processed}/{max_alerts}")

                        if alerts_processed >= max_alerts:
                            break
                    else:
                        alerts_skipped += 1
                        # Show progress every 10 skipped alerts
                        if alerts_skipped % 10 == 0:
                            print(f"Examined {alerts_examined} alerts, processed {alerts_processed}, skipped {alerts_skipped}...", end='\r')

                except json.JSONDecodeError:
                    print(f"Warning: Could not parse alert as JSON")
                except Exception as e:
                    print(f"\n❌ ERROR processing alert: {e}")
                    import traceback
                    traceback.print_exc()

    except KeyboardInterrupt:
        print("\n\nStopped by user")

    print("\n" + "="*80)
    print(f"SUMMARY")
    print("="*80)
    print(f"Total alerts examined: {alerts_examined}")
    print(f"Alerts matching filter: {alerts_processed}")
    print(f"Alerts skipped: {alerts_skipped}")
    print(f"Mode: {'DRY RUN (no API calls)' if dry_run else 'LIVE (API calls made)'}")
    print("="*80)


def discover_alert_files(superevent_id):
    """
    Discover all available alert JSON files for a superevent from GraceDB.

    Args:
        superevent_id: The superevent ID (e.g., 'S251010al')

    Returns:
        List of alert filenames (e.g., ['S251017at-preliminary.json', 'S251017at-update.json'])
    """
    files_url = f"https://gracedb.ligo.org/api/superevents/{superevent_id}/files/"

    try:
        response = requests.get(files_url)
        if response.status_code != 200:
            print(f"⚠️  Could not list files for {superevent_id} (HTTP {response.status_code})")
            return []

        # Parse the JSON response - GraceDB returns a dict where keys are filenames
        files_data = response.json()

        # Filter for alert JSON files (common alert type names)
        alert_types = ['earlywarning', 'preliminary', 'initial', 'update', 'retraction']
        alert_files = []

        for filename in files_data.keys():
            # Check if it's a JSON file
            if not filename.endswith('.json'):
                continue

            # Extract the alert type from filename (format: S251017at-preliminary.json)
            # Remove superevent ID prefix and .json suffix
            basename = filename.lower()
            for alert_type in alert_types:
                if f"-{alert_type}.json" in basename:
                    alert_files.append(filename)
                    break

        return sorted(alert_files)

    except requests.RequestException as e:
        print(f"⚠️  Network error listing files for {superevent_id}: {e}")
        return []
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        print(f"⚠️  Could not parse file list for {superevent_id}: {e}")
        return []


def ingest_from_gracedb(config_path, superevent_ids, alert_types=None,
                        write_to_s3=False, dry_run=True, listener_type="LIGO_ALERT"):
    """
    Download and ingest alerts directly from GraceDB URLs.

    Args:
        config_path: Path to listener config JSON
        superevent_ids: List of superevent IDs (e.g., ['S251010al', 'S251011x'])
        alert_types: List of alert types to download (e.g., ['preliminary', 'update'])
                     If None, automatically discovers all available alert files
        write_to_s3: Whether to write to S3 storage
        dry_run: If True, don't post to API
        listener_type: Type of listener (LIGO_ALERT or ICECUBE_NOTICE)
    """
    print("="*80)
    print(f"GRACEDB DOWNLOAD MODE - {listener_type}")
    print("="*80)
    print(f"Superevent IDs: {', '.join(superevent_ids)}")
    if alert_types:
        print(f"Alert types (specified): {', '.join(alert_types)}")
    else:
        print(f"Alert types: Auto-discover all available")
    print(f"Write to S3: {write_to_s3}")
    print(f"Dry run: {dry_run} (API posting {'DISABLED' if dry_run else 'ENABLED'})")
    print()

    # Create listener
    l = listener.Listener(listener_type=listener_type, config_path=config_path)

    alerts_processed = 0
    alerts_failed = 0
    alerts_skipped = 0

    for superevent_id in superevent_ids:
        print(f"\n{'='*60}")
        print(f"Processing superevent: {superevent_id}")
        print(f"{'='*60}")

        # If alert_types not specified, discover what's available
        if alert_types is None:
            print("Discovering available alert files...")
            alert_files = discover_alert_files(superevent_id)
            if not alert_files:
                print(f"⚠️  No alert files found for {superevent_id}")
                continue
            print(f"Found {len(alert_files)} alert file(s): {', '.join(alert_files)}")
        else:
            # Use specified alert types - construct full filenames
            alert_files = [f"{superevent_id}-{atype}.json" for atype in alert_types]

        # Download and process each alert file
        for filename in alert_files:
            url = f"https://gracedb.ligo.org/api/superevents/{superevent_id}/files/{filename}"

            print(f"\n  → Downloading: {filename}")
            print(f"    URL: {url}")

            try:
                # Download the alert JSON
                response = requests.get(url)

                if response.status_code == 404:
                    print(f"    ⚠️  Not found - skipping")
                    alerts_skipped += 1
                    continue
                elif response.status_code != 200:
                    print(f"    ❌ HTTP {response.status_code} - skipping")
                    alerts_failed += 1
                    continue

                # Parse to verify it's valid JSON
                alert_json = response.text
                alert_data = json.loads(alert_json)

                print(f"    ✓ Downloaded {len(alert_json)} bytes")
                print(f"      Alert type: {alert_data.get('alert_type', 'UNKNOWN')}")
                print(f"      Time created: {alert_data.get('time_created', 'UNKNOWN')}")

                # Process the alert through the listener
                alert, ext_alert = l._listen(
                    alert=alert_json,
                    write_to_storage=write_to_s3,
                    verbose=True,
                    dry_run=dry_run
                )

                print(f"\n    Result:")
                print(f"    {alert}")
                if ext_alert:
                    print(f"\n    External Alert:")
                    print(f"    {ext_alert}")

                alerts_processed += 1
                print(f"\n    ✓ Processed successfully")

            except requests.RequestException as e:
                print(f"    ❌ Network error: {e}")
                alerts_failed += 1
            except json.JSONDecodeError as e:
                print(f"    ❌ Invalid JSON: {e}")
                alerts_failed += 1
            except Exception as e:
                print(f"    ❌ Processing error: {e}")
                import traceback
                traceback.print_exc()
                alerts_failed += 1

    print("\n" + "="*80)
    print(f"SUMMARY")
    print("="*80)
    print(f"Alerts processed: {alerts_processed}")
    print(f"Alerts skipped: {alerts_skipped}")
    print(f"Alerts failed: {alerts_failed}")
    print(f"Mode: {'DRY RUN (no API calls)' if dry_run else 'LIVE (API calls made)'}")
    print("="*80)


def main():
    parser = argparse.ArgumentParser(
        description='Selective alert ingestion tool for GWTM',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List available alerts (first 50)
  %(prog)s --list --max 50

  # List IceCube alerts
  %(prog)s --list --type ICECUBE_NOTICE

  # Test what would be ingested (dry run)
  %(prog)s --ingest --filter "S230"

  # Actually ingest alerts containing "S230", excluding test alerts
  %(prog)s --ingest --filter "S230" --exclude-test --no-dry-run

  # Ingest and write to S3
  %(prog)s --ingest --filter "S230" --write --no-dry-run

  # Ingest all S25* alerts except test alerts (MS*)
  %(prog)s --ingest --filter "S25" --exclude-test --write --no-dry-run

  # Download specific alerts from GraceDB (dry run)
  %(prog)s --gracedb --ids S251010al S251011x

  # Download and ingest from GraceDB with specific alert types
  %(prog)s --gracedb --ids S251010al --alert-types preliminary update --no-dry-run --write

  # Download all alert types for multiple events
  %(prog)s --gracedb --ids S251010al S251011x --alert-types preliminary initial update retraction --write --no-dry-run
        """
    )

    # Mode selection
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument('--list', action='store_true',
                           help='List mode: show available alerts without ingesting')
    mode_group.add_argument('--ingest', action='store_true',
                           help='Ingest mode: process alerts matching filter')
    mode_group.add_argument('--gracedb', action='store_true',
                           help='GraceDB mode: download alerts directly from GraceDB URLs')

    # Common options
    parser.add_argument('--config', type=str, default='./listener_config.json',
                       help='Path to listener config file (default: ./listener_config.json)')
    parser.add_argument('--type', type=str, default='LIGO_ALERT',
                       choices=['LIGO_ALERT', 'ICECUBE_NOTICE'],
                       help='Listener type (default: LIGO_ALERT)')
    parser.add_argument('--max', type=int, default=50,
                       help='Maximum number of alerts to process (default: 50)')
    parser.add_argument('--timeout', type=int, default=60,
                       help='Timeout in seconds (default: 60)')
    parser.add_argument('--debug', action='store_true',
                       help='Show raw JSON for first few alerts (debug mode)')
    parser.add_argument('--exclude-test', action='store_true',
                       help='Exclude test alerts (MS* alerts)')

    # Ingest mode options
    parser.add_argument('--filter', type=str,
                       help='[INGEST] Only process alerts containing this string')
    parser.add_argument('--write', action='store_true',
                       help='[INGEST/GRACEDB] Write to S3 storage')
    parser.add_argument('--no-dry-run', action='store_true',
                       help='[INGEST/GRACEDB] Actually post to API (default is dry run)')

    # GraceDB mode options
    parser.add_argument('--ids', type=str, nargs='+',
                       help='[GRACEDB] Superevent IDs to download (e.g., S251010al S251011x)')
    parser.add_argument('--alert-types', type=str, nargs='+',
                       help='[GRACEDB] Alert types to download (e.g., preliminary update). Default: preliminary initial update')

    args = parser.parse_args()

    # Validate config file exists
    if not os.path.exists(args.config):
        print(f"❌ ERROR: Config file not found: {args.config}")
        print(f"\nPlease ensure your listener config exists at:")
        print(f"  {os.path.abspath(args.config)}")
        return 1

    try:
        if args.list:
            # List mode
            list_alerts(
                config_path=args.config,
                listener_type=args.type,
                max_alerts=args.max,
                timeout_seconds=args.timeout,
                debug=args.debug
            )
        elif args.ingest:
            # Ingest mode
            if not args.filter:
                print("❌ ERROR: --filter is required for ingest mode")
                return 1

            ingest_filtered(
                config_path=args.config,
                listener_type=args.type,
                filter_string=args.filter,
                write_to_s3=args.write,
                dry_run=not args.no_dry_run,
                max_alerts=args.max,
                timeout_seconds=args.timeout,
                exclude_test=args.exclude_test
            )
        elif args.gracedb:
            # GraceDB mode
            if not args.ids:
                print("❌ ERROR: --ids is required for gracedb mode")
                print("Example: --gracedb --ids S251010al S251011x")
                return 1

            ingest_from_gracedb(
                config_path=args.config,
                superevent_ids=args.ids,
                alert_types=args.alert_types,
                write_to_s3=args.write,
                dry_run=not args.no_dry_run,
                listener_type=args.type
            )

        return 0

    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == '__main__':
    sys.exit(main())
