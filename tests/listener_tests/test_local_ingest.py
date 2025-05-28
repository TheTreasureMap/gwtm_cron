import sys
import os

sys.path.insert(0, '../../src/')

import gwtm_cron.gwtm_listener as gl # type: ignore

def test_local_file_ingestion():
    listener = gl.listener.Listener(
        listener_type="LIGO_ALERT",
        config_path="/Users/sdwyatt/cron/listener_config.json"
    )
    alert_dir = "alerts"
    files = os.listdir(os.path.join(os.getcwd(), alert_dir))
    files = [x for x in files if 'DS_Store' not in x]
    for f in sorted(files, reverse=True):
        print(f)
        pw = os.path.join(os.getcwd(), alert_dir, f)
        listener.local_run(
            alert_json_path=pw,
            dry_run=False,
            write_to_s3=True,
        )


if __name__ == '__main__':
    test_local_file_ingestion()
