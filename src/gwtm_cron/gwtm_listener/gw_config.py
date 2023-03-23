import os

class Config(object):
    AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', '')
    AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
    AWS_DEFAULT_REGION = os.environ.get('AWS_DEFAULT_REGION', 'us-east-2')
    AWS_BUCKET = os.environ.get('AWS_BUCKET', 'gwtreasuremap')
    API_TOKEN = os.environ.get('API_TOKEN', '')
    API_BASE = os.environ.get('API_BASE', 'http://127.0.0.1:5000/api/v0/')
    ALERT_DOMAIN = os.environ.get('ALERT_DOMAIN', 'igwn.gwalert')
    OBSERVING_RUN = os.environ.get('OBSERVING_RUN', 'O4')
    KAFKA_CLIENT_ID = os.environ.get('KAFKA_CLIENT_ID', '')
    KAFKA_CLIENT_SECRET = os.environ.get('KAFKA_CLIENT_SECRET', '')

config = Config()

