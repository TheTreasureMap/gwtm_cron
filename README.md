# gwtm_cron

Gravitational Wave Treasure Map cron functions - a Kafka-based listener system for processing:
* LIGO/Virgo/KAGRA gravitational wave alerts
* IceCube neutrino coincidence notices

**Requirements**: Python 3.11+

## How It Works

The listeners are **real-time streaming processors** that:

1. **Subscribe to NASA GCN Kafka streams**
   - LIGO Listener: `igwn.gwalert` topic for gravitational wave detections
   - IceCube Listener: `gcn.notices.icecube.lvk_nu_track_search` for neutrino coincidences

2. **Process each alert** as it arrives (typically within seconds of detection):
   - Parse alert JSON and extract metadata (event ID, classification, instruments)
   - Decode and analyze probability skymaps (FITS format)
   - Calculate sky localization statistics (90%/50% credible areas, average position)
   - Generate derived products:
     - Sky contours (GeoJSON for visualization)
     - MOC (Multi-Order Coverage) files
     - Satellite visibility maps (Fermi, LAT)
   - Query galaxy catalogs to identify potential host galaxies

3. **Store products** to cloud storage (S3/Azure/OpenStack Swift):
   - Raw alert JSON
   - Processed FITS skymaps
   - Visualization-ready contours and maps
   - One event can produce 5-10 files as it evolves (Early Warning → Preliminary → Update)

4. **POST to GWTM API** for public consumption by astronomers worldwide

**Important**: Listeners only process **new alerts** that arrive after they start. Historical alerts are not backfilled. If you start with empty storage, it will remain empty until the next gravitational wave or neutrino detection is announced.

## Quick Start

### Docker Compose (Local Development)

```bash
# Build and run both listeners
docker compose up

# Run specific listener
docker compose up ligo-listener
docker compose up icecube-listener
```

### Manual Docker Build

```bash
docker build -t gwtm_cron .
docker tag gwtm_cron:latest ghcr.io/thetreasuremap/gwtm_cron:latest
docker push ghcr.io/thetreasuremap/gwtm_cron:latest
```

Images are published to `ghcr.io/thetreasuremap/gwtm_cron` via GitHub Actions.

## Production Deployment

Listeners are deployed to Kubernetes via ArgoCD, managed from the
[gwtm-deploy](https://github.com/TheTreasureMap/gwtm-deploy) repo and using the
Helm chart in the [gwtm](https://github.com/TheTreasureMap/gwtm) repo
(`gwtm-helm/values-listeners-prod.yaml`).

### Releasing a new version

1. Merge changes to `master`
2. Tag the release:
   ```bash
   git tag v1.2.3
   git push origin v1.2.3
   ```
3. GitHub Actions builds and pushes `ghcr.io/thetreasuremap/gwtm_cron:1.2.3`
4. ArgoCD Image Updater detects the new semver tag and automatically updates the
   Helm parameter `listeners.image.tag` in the `gwtm` repo, writing back to
   `gwtm-helm/.argocd-source-gwtm-listeners.yaml`
5. ArgoCD syncs the `gwtm-listeners` application and rolls out the new pods

No manual Helm or kubectl commands are needed for routine releases.

## Environment Variables

The following environment variables are required for Docker/Kubernetes/Helm deployments:

### Required Variables

#### Kafka Configuration
- `KAFKA_CLIENT_ID` - GCN Kafka client ID for authentication
- `KAFKA_CLIENT_SECRET` - GCN Kafka client secret

#### GWTM API Configuration
- `API_TOKEN` - Authentication token for GWTM API
- `API_BASE` - Base URL for GWTM API (e.g., `https://treasuremap.space/api/v0/`)

#### Cloud Storage Configuration

**Option 1: AWS S3**
- `AWS_ACCESS_KEY_ID` - AWS access key
- `AWS_SECRET_ACCESS_KEY` - AWS secret key
- `AWS_DEFAULT_REGION` - AWS region (default: `us-east-2`)
- `AWS_BUCKET` - S3 bucket name (default: `gwtreasuremap`)
- `STORAGE_BUCKET_SOURCE=s3` - Set to `s3` for AWS storage

**Option 2: Azure Blob Storage**
- `AZURE_ACCOUNT_NAME` - Azure storage account name
- `AZURE_ACCOUNT_KEY` - Azure storage account key
- `STORAGE_BUCKET_SOURCE=abfs` - Set to `abfs` for Azure storage

**Option 3: OpenStack Swift**
- `OS_AUTH_URL` - OpenStack authentication endpoint (e.g., `https://openstack.example.com:5000/v3`)
- `OS_USERNAME` - OpenStack username
- `OS_PASSWORD` - OpenStack password
- `OS_PROJECT_NAME` - OpenStack project/tenant name
- `OS_USER_DOMAIN_NAME` - User domain name (default: `Default`)
- `OS_PROJECT_DOMAIN_NAME` - Project domain name (default: `Default`)
- `OS_CONTAINER_NAME` - Swift container name (default: `gwtreasuremap`)
- `STORAGE_BUCKET_SOURCE=swift` - Set to `swift` for OpenStack storage

### Optional Variables

- `OBSERVING_RUN` - Observing run identifier (default: `O4`)
- `PATH_TO_GALAXY_CATALOG_CONFIG` - Path to galaxy catalog config file (only needed for LIGO listener if generating galaxy lists)

### Listener Control Variables

- `DRY_RUN` - Set to `true` or `1` to run in dry-run mode (no API calls, no storage writes)
- `WRITE_TO_STORAGE` - Set to `false` or `0` to disable storage writes (default: `true`)
- `VERBOSE` - Set to `false` or `0` to disable verbose logging (default: `true`)

**Note**: Storage type (S3, Azure, Swift) is controlled by `STORAGE_BUCKET_SOURCE`, not by `WRITE_TO_STORAGE`.

### Logging Configuration

- `LOG_FORMAT` - Set to `json` to enable structured JSON logging for Kubernetes (default: print statements)
- `LOG_LEVEL` - Set log level: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` (default: `INFO`)

### Example Kubernetes/Helm Values

**AWS S3 Example:**
```yaml
env:
  - name: KAFKA_CLIENT_ID
    valueFrom:
      secretKeyRef:
        name: gwtm-secrets
        key: kafka-client-id
  - name: KAFKA_CLIENT_SECRET
    valueFrom:
      secretKeyRef:
        name: gwtm-secrets
        key: kafka-client-secret
  - name: API_TOKEN
    valueFrom:
      secretKeyRef:
        name: gwtm-secrets
        key: api-token
  - name: API_BASE
    value: "https://treasuremap.space/api/v0/"
  - name: STORAGE_BUCKET_SOURCE
    value: "s3"
  - name: AWS_ACCESS_KEY_ID
    valueFrom:
      secretKeyRef:
        name: aws-credentials
        key: access-key-id
  - name: AWS_SECRET_ACCESS_KEY
    valueFrom:
      secretKeyRef:
        name: aws-credentials
        key: secret-access-key
  - name: AWS_DEFAULT_REGION
    value: "us-east-2"
  - name: AWS_BUCKET
    value: "gwtreasuremap"
  - name: OBSERVING_RUN
    value: "O4"
```

**OpenStack Swift Example:**
```yaml
env:
  - name: KAFKA_CLIENT_ID
    valueFrom:
      secretKeyRef:
        name: gwtm-secrets
        key: kafka-client-id
  - name: KAFKA_CLIENT_SECRET
    valueFrom:
      secretKeyRef:
        name: gwtm-secrets
        key: kafka-client-secret
  - name: API_TOKEN
    valueFrom:
      secretKeyRef:
        name: gwtm-secrets
        key: api-token
  - name: API_BASE
    value: "https://treasuremap.space/api/v0/"
  - name: STORAGE_BUCKET_SOURCE
    value: "swift"
  - name: OS_AUTH_URL
    value: "https://openstack.example.com:5000/v3"
  - name: OS_USERNAME
    valueFrom:
      secretKeyRef:
        name: openstack-credentials
        key: username
  - name: OS_PASSWORD
    valueFrom:
      secretKeyRef:
        name: openstack-credentials
        key: password
  - name: OS_PROJECT_NAME
    value: "gwtm-project"
  - name: OS_USER_DOMAIN_NAME
    value: "Default"
  - name: OS_PROJECT_DOMAIN_NAME
    value: "Default"
  - name: OS_CONTAINER_NAME
    value: "gwtreasuremap"
  - name: OBSERVING_RUN
    value: "O4"
```

## Deployment Architecture

The system runs two independent listener processes:

1. **LIGO Listener** (`docker/run_ligo_listener.py`)
   - Subscribes to `igwn.gwalert` Kafka topic
   - Processes gravitational wave alerts
   - Generates skymaps, contours, and galaxy lists
   - Posts to GWTM API

2. **IceCube Listener** (`docker/run_icecube_listener.py`)
   - Subscribes to `gcn.notices.icecube.lvk_nu_track_search` Kafka topic
   - Processes neutrino coincidence notices
   - Posts to GWTM API

Both listeners run continuously and process alerts in real-time as they arrive on the Kafka stream.

## Data Migration

When migrating between storage backends (e.g., moving from AWS to OpenStack):

```bash
# Dry run to see what would be migrated (includes size estimation)
python scripts/migrate_storage.py --source s3 --dest swift --container fit --dry-run

# Actual migration with progress tracking
python scripts/migrate_storage.py --source s3 --dest swift --container fit

# Migrate test data
python scripts/migrate_storage.py --source s3 --dest swift --container test
```

**Migration Features**:
- **Size Estimation**: Calculates total data size before transfer (samples files if >100)
- **Time Estimation**: Shows ETA and transfer rate during migration
- **Progress Tracking**: Real-time progress with percentage complete
- **Transfer Statistics**: Reports total size, time, and average transfer rate
- **Error Handling**: Continues on errors and reports failed files at end
- **Dry-run Mode**: Preview migration without transferring data

**Example Output**:
```
Scanning source (s3)...
Found 1523 files. Calculating total size...
Total size: ~4.23 GB (1523 files)
--------------------------------------------------------------------------------

[1/1523 - 0.1%] fit/S230518h-Preliminary.fits.gz
  Size: 2.45 MB, Time: 1.2s, Rate: 2.04 MB/s, ETA: 30.5m
[2/1523 - 0.1%] fit/S230518h-contours-smooth.json (145.23 KB) ✓
...
[1523/1523 - 100.0%] test/MS181101ab-retraction.json (8.12 KB) ✓

================================================================================
Migration complete!
  Successful: 1523/1523
  Total size: 4.23 GB
  Total time: 28.3m
  Avg rate: 2.56 MB/s
```

## Testing

```bash
# Run local ingestion tests with sample alerts
python tests/listener_tests/test_local_ingest.py
python tests/icecube_tests/test_local_ingest.py
```

