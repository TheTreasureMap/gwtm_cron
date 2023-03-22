import sys
import os
import tempfile
import json 

sys.path.insert(0, '../../src/')

import gwtm_cron.gwtm_listener as gl

def test_local_file_ingestion():
    listener = gl.listener.Listener()
    reader = gl.listener.io.Reader(read_from_s3=True)
    test_file_path = 'test/MS230322u-Preliminary_alert.json'
    test_file = reader.read_alert_json(test_file_path, listener.config, verbose=True)
    
    tmp = tempfile.NamedTemporaryFile()
    with open(tmp.name, 'w') as f:
        f.write(json.dumps(test_file))

    listener.local_run(
        alert_json_path=tmp.name,
        dry_run=True,
        write_to_s3=False
    )


if __name__ == '__main__':
    test_local_file_ingestion()
