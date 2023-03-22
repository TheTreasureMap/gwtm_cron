import sys

sys.path.insert(0, '../../src/')

import gwtm_cron.gwtm_listener as gl


def test_kafka_stream():
    listener = gl.listener.Listener()
    listener.run(write_to_s3=True, verbose=True, dry_run=False)


if __name__ == '__main__':
    test_kafka_stream()
