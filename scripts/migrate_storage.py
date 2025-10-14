#!/usr/bin/env python3
"""
Storage Migration Script for GWTM Cron

Migrates data between different storage backends (S3, Azure, OpenStack Swift).
Useful for migrating existing data when moving to a new deployment environment.

Usage:
    python scripts/migrate_storage.py --source s3 --dest swift --container fit
    python scripts/migrate_storage.py --source azure --dest swift --container test --dry-run

Requirements:
    - Source and destination credentials must be configured via environment variables or config file
    - Large transfers may take significant time
"""

import argparse
import sys
import os
import time
from pathlib import Path
from datetime import timedelta

# Add parent directory to path to import gwtm_cron modules
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from gwtm_cron.gwtm_listener import gwstorage, gw_config


def format_size(bytes_size):
    """Convert bytes to human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if bytes_size < 1024.0:
            return f"{bytes_size:.2f} {unit}"
        bytes_size /= 1024.0
    return f"{bytes_size:.2f} PB"


def format_time(seconds):
    """Convert seconds to human-readable format."""
    if seconds < 60:
        return f"{seconds:.1f}s"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f}m"
    else:
        return str(timedelta(seconds=int(seconds)))


def migrate_container(source, dest, container, config, dry_run=False, verbose=True):
    """
    Migrate all files from one storage backend to another.

    Args:
        source: Source storage type ('s3', 'abfs', 'swift')
        dest: Destination storage type ('s3', 'abfs', 'swift')
        container: Container/path to migrate (e.g., 'fit', 'test')
        config: Config object with credentials
        dry_run: If True, only list files without transferring
        verbose: Print progress messages
    """
    if verbose:
        print(f"{'DRY RUN: ' if dry_run else ''}Migrating container '{container}' from {source} to {dest}")
        print("-" * 80)

    start_time = time.time()

    try:
        # List all files in source container
        if verbose:
            print(f"Scanning source ({source})...")
        files = gwstorage.list_gwtm_bucket(container, source=source, config=config)

        # Calculate total size by sampling files
        if verbose:
            print(f"Found {len(files)} files. Calculating total size...")

        total_size = 0
        sample_count = 0

        # Get sizes for all files (or sample if too many)
        files_to_sample = files if len(files) <= 100 else files[::max(1, len(files) // 100)]

        for filename in files_to_sample:
            try:
                content = gwstorage.download_gwtm_file(
                    filename,
                    source=source,
                    config=config,
                    decode=False
                )
                total_size += len(content)
                sample_count += 1
            except Exception:
                # Skip files that can't be accessed
                pass

        # Estimate total size if we sampled
        if len(files) > sample_count:
            avg_size = total_size / sample_count if sample_count > 0 else 0
            estimated_total = avg_size * len(files)
            is_estimate = True
        else:
            estimated_total = total_size
            is_estimate = False

        if verbose:
            size_str = format_size(estimated_total)
            print(f"Total size: {'~' if is_estimate else ''}{size_str} ({len(files)} files)")
            print("-" * 80)

            if dry_run:
                print("\nFiles to be migrated:")
                for f in files:
                    print(f"  - {f}")
                print(f"\n{'~' if is_estimate else ''}{size_str} would be transferred")
                return len(files), estimated_total

        # Transfer each file
        migrated = 0
        failed = []
        bytes_transferred = 0
        last_update_time = start_time

        for i, filename in enumerate(files, 1):
            try:
                file_start = time.time()

                if not dry_run:
                    # Download from source
                    content = gwstorage.download_gwtm_file(
                        filename,
                        source=source,
                        config=config,
                        decode=False
                    )

                    file_size = len(content)
                    bytes_transferred += file_size

                    # Upload to destination
                    gwstorage.upload_gwtm_file(
                        content,
                        filename,
                        source=dest,
                        config=config
                    )

                    file_time = time.time() - file_start

                    if verbose:
                        # Calculate progress
                        elapsed = time.time() - start_time
                        progress_pct = (i / len(files)) * 100
                        avg_time_per_file = elapsed / i
                        remaining_files = len(files) - i
                        eta_seconds = remaining_files * avg_time_per_file

                        # Calculate transfer rate
                        transfer_rate = bytes_transferred / elapsed if elapsed > 0 else 0

                        # Print progress every file, but update ETA every 5 seconds
                        current_time = time.time()
                        if current_time - last_update_time >= 5 or i == 1 or i == len(files):
                            print(f"[{i}/{len(files)} - {progress_pct:.1f}%] {filename}")
                            print(f"  Size: {format_size(file_size)}, Time: {format_time(file_time)}, "
                                  f"Rate: {format_size(transfer_rate)}/s, ETA: {format_time(eta_seconds)}")
                            last_update_time = current_time
                        else:
                            print(f"[{i}/{len(files)} - {progress_pct:.1f}%] {filename} ({format_size(file_size)}) ✓")
                else:
                    if verbose:
                        print(f"[{i}/{len(files)}] {filename}")

                migrated += 1

            except Exception as e:
                if verbose:
                    print(f"[{i}/{len(files)}] {filename} ✗ Error: {e}")
                failed.append((filename, str(e)))

        # Summary
        elapsed_total = time.time() - start_time

        if verbose:
            print("\n" + "=" * 80)
            print(f"Migration complete!")
            print(f"  Successful: {migrated}/{len(files)}")
            print(f"  Total size: {format_size(bytes_transferred)}")
            print(f"  Total time: {format_time(elapsed_total)}")
            if elapsed_total > 0:
                print(f"  Avg rate: {format_size(bytes_transferred / elapsed_total)}/s")
            if failed:
                print(f"  Failed: {len(failed)}")
                print("\nFailed files:")
                for fname, error in failed:
                    print(f"  - {fname}: {error}")

        return migrated, bytes_transferred

    except Exception as e:
        print(f"Error during migration: {e}")
        return 0, 0


def main():
    parser = argparse.ArgumentParser(
        description="Migrate GWTM storage data between backends",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Migrate from S3 to OpenStack Swift (dry run)
  python scripts/migrate_storage.py --source s3 --dest swift --container fit --dry-run

  # Migrate from Azure to OpenStack Swift (actual migration)
  python scripts/migrate_storage.py --source abfs --dest swift --container fit

  # Migrate specific container with config file
  python scripts/migrate_storage.py --source s3 --dest swift --container test --config /path/to/config.json

Storage Backend Options:
  s3    - AWS S3
  abfs  - Azure Blob Storage
  swift - OpenStack Swift

Environment Variables Required:
  Source S3:     AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_BUCKET
  Source Azure:  AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY
  Source Swift:  OS_AUTH_URL, OS_USERNAME, OS_PASSWORD, OS_PROJECT_NAME, OS_CONTAINER_NAME

  Dest S3:       AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_BUCKET
  Dest Azure:    AZURE_ACCOUNT_NAME, AZURE_ACCOUNT_KEY
  Dest Swift:    OS_AUTH_URL, OS_USERNAME, OS_PASSWORD, OS_PROJECT_NAME, OS_CONTAINER_NAME
        """
    )

    parser.add_argument("--source", required=True, choices=["s3", "abfs", "swift"],
                        help="Source storage backend")
    parser.add_argument("--dest", required=True, choices=["s3", "abfs", "swift"],
                        help="Destination storage backend")
    parser.add_argument("--container", required=True,
                        help="Container/directory to migrate (e.g., 'fit', 'test')")
    parser.add_argument("--config", type=str,
                        help="Path to JSON config file (optional, uses env vars if not provided)")
    parser.add_argument("--dry-run", action="store_true",
                        help="List files without actually migrating them")
    parser.add_argument("--quiet", action="store_true",
                        help="Suppress progress output")

    args = parser.parse_args()

    # Validation
    if args.source == args.dest:
        print("Error: Source and destination must be different")
        sys.exit(1)

    # Load configuration
    config = gw_config.Config(path_to_config=args.config)

    # Validate credentials are available
    if args.source == "s3" or args.dest == "s3":
        if not config.AWS_ACCESS_KEY_ID or not config.AWS_BUCKET:
            print("Error: AWS credentials not configured")
            sys.exit(1)

    if args.source == "abfs" or args.dest == "abfs":
        if not config.AZURE_ACCOUNT_NAME:
            print("Error: Azure credentials not configured")
            sys.exit(1)

    if args.source == "swift" or args.dest == "swift":
        if not config.OS_AUTH_URL or not config.OS_USERNAME:
            print("Error: OpenStack Swift credentials not configured")
            sys.exit(1)

    # Perform migration
    try:
        migrated, bytes_transferred = migrate_container(
            source=args.source,
            dest=args.dest,
            container=args.container,
            config=config,
            dry_run=args.dry_run,
            verbose=not args.quiet
        )

        if args.dry_run and not args.quiet:
            print(f"\nDry run complete. {migrated} files ({format_size(bytes_transferred)}) would be migrated.")
            print("Run without --dry-run to perform actual migration.")

        sys.exit(0)

    except KeyboardInterrupt:
        print("\n\nMigration interrupted by user")
        sys.exit(1)
    except Exception as e:
        print(f"\nMigration failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
