import sys
import os

sys.path.insert(0, '../../src/')

import gwtm_cron.gwtm_listener as gl

def test_local_file_ingestion():
    listener = gl.listener.Listener()
    files = [
        'alerts/MS181101ab-earlywarning.json',
        'alerts/MS181101ab-preliminary.json',
        'alerts/MS181101ab-initial.json',
        'alerts/MS181101ab-retraction.json',
        'alerts/MS181101ab-update.json'
    ]
    for f in files:
        pw = os.path.join(os.getcwd(), f)
        listener.local_run(
            alert_json_path=pw,
            dry_run=False, 
            write_to_s3=False
        )


if __name__ == '__main__':
    test_local_file_ingestion()
