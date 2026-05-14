# gwtm_cron
Gravitational Wave Treasure Map cron functions
* GCN Listener
* Others coming soon

### Build/Deploy with docker
```bash

docker compose up

docker build -t gwtm_cron .

docker tag gwtm_cron:latest 929887798640.dkr.ecr.us-east-2.amazonaws.com/gwtm_cron_listener:latest

./ecrlogin.sh

docker push 929887798640.dkr.ecr.us-east-2.amazonaws.com/gwtm_cron_listener:latest
```

## Selective Alert Ingestion

The `selective_ingest.py` tool provides flexible ways to catch up on gravitational wave alerts by either filtering the Kafka stream or downloading directly from GraceDB.

### Features

- **List Mode**: View available alerts from the Kafka stream without processing them
- **Ingest Mode**: Process alerts from Kafka matching a specific filter string
- **GraceDB Mode**: Download and ingest specific alerts directly from GraceDB URLs (recommended)

### GraceDB Mode (Recommended)

Download alerts directly from GraceDB by superevent ID. This automatically discovers and downloads all available alert types (earlywarning, preliminary, initial, update, retraction).

```bash
# Dry run - test what would be ingested (no API posting)
python selective_ingest.py --gracedb --ids S251017at

# Production - ingest specific events with S3 upload and API posting
python selective_ingest.py --gracedb --ids S251017at S251010al --write --no-dry-run

# Ingest only specific alert types
python selective_ingest.py --gracedb --ids S251017at --alert-types preliminary update --write --no-dry-run
```

### Kafka Stream Modes

List available alerts in the Kafka stream:
```bash
python selective_ingest.py --list --max 50
```

Process alerts matching a filter string:
```bash
# Dry run
python selective_ingest.py --ingest --filter "S2510"

# Production
python selective_ingest.py --ingest --filter "S2510" --exclude-test --write --no-dry-run
```

### Configuration

The tool uses the same configuration as the main listener. By default it looks for `./listener_config.json`, or you can specify a path:

```bash
python selective_ingest.py --gracedb --ids S251017at --config /path/to/config.json --write --no-dry-run
```

### Notes

- Use `--write` to upload skymaps and contours to S3/Azure storage
- Use `--no-dry-run` to actually post to the GWTM API (default is dry run)
- Use `--exclude-test` to skip test alerts (MS* events) when filtering Kafka
- GraceDB mode automatically discovers all available alert files for each event


