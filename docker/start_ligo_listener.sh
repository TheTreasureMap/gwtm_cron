#!/bin/bash
set -e

CATALOG_DIR=/app/data
CATALOG_FILE=NEDLVS_20250602.fits
CATALOG_PATH="${CATALOG_DIR}/${CATALOG_FILE}"

mkdir -p "$CATALOG_DIR"

if [ ! -f "$CATALOG_PATH" ]; then
    echo "Downloading galaxy catalog from Swift..."
    python3 - <<EOF
from keystoneauth1.identity import v3
from keystoneauth1 import session
from swiftclient import Connection as SwiftConnection
import os

auth = v3.ApplicationCredential(
    auth_url=os.environ['OS_AUTH_URL'],
    application_credential_id=os.environ['OS_USERNAME'],
    application_credential_secret=os.environ['OS_PASSWORD']
)
sess = session.Session(auth=auth)
conn = SwiftConnection(
    session=sess,
    os_options={'object_storage_url': os.environ['OS_STORAGE_URL']}
)

headers, body = conn.get_object(
    os.environ['OS_CONTAINER_NAME'],
    'galaxy_catalog/${CATALOG_FILE}',
    resp_chunk_size=64*1024*1024
)
with open('${CATALOG_PATH}', 'wb') as f:
    for chunk in body:
        f.write(chunk)
print('Download complete.')
EOF
else
    echo "Galaxy catalog already present, skipping download."
fi

exec python docker/run_ligo_listener.py "$@"
