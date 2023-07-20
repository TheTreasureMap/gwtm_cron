import sys
import os

sys.path.insert(0, '../../src/')

import gwtm_cron.gwtm_listener as gl

def test_local_file_ingestion():
    listener = gl.listener.Listener(listener_type="ICECUBE_NOTICE")
    alert_dir = "alerts"
    files = os.listdir(os.path.join(os.getcwd(), alert_dir))
    #files = [x for x in files if 'retraction' in x]
    for f in sorted(files, reverse=True):
        print(f)
        pw = os.path.join(os.getcwd(), alert_dir, f)
        listener.local_run(
            alert_json_path=pw,
            dry_run=True,
            write_to_s3=False,
        )


if __name__ == '__main__':
    test_local_file_ingestion()
