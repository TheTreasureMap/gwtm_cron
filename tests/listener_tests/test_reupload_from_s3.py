import sys
import tempfile
import json
import boto3 # type: ignore

sys.path.insert(0, '../../src/')

import gwtm_cron.gwtm_listener as gl # type: ignore

def test_local_file_ingestion():
    listener = gl.listener.Listener(
        listener_type="LIGO_ALERT",
        config_path="/Users/sdwyatt/cron/listener_config.json"
    )
    reader = gl.listener.io.Reader(read_from_s3=True)

    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket(listener.config.AWS_BUCKET)
    objects = bucket.objects.filter(Prefix = 'test/')
    objects_to_upload = [
        o.key for o in objects if 'alert.json' in o.key and o.key != 'test/'
	]

    culled_objects = [
        x for x in objects_to_upload if 'MS230401' in x or 'MS230413' in x
    ]


    for co in culled_objects:
        test_file_path = co
        test_file = reader.read_alert_json(test_file_path, listener.config, verbose=True)

        tmp = tempfile.NamedTemporaryFile()
        with open(tmp.name, 'w') as f:
            f.write(json.dumps(test_file))

        listener.local_run(
            alert_json_path=tmp.name,
            dry_run=False,
            write_to_s3=False,
        )


if __name__ == '__main__':
    test_local_file_ingestion()
