import os
import json

class Config(object):

    def __init__(self, path_to_config=None):
        print(path_to_config)
        if path_to_config and os.path.exists(path_to_config):

            fi = open(path_to_config)
            data = json.load(fi)

            self.AWS_ACCESS_KEY_ID = data["AWS_ACCESS_KEY_ID"] if "AWS_ACCESS_KEY_ID" in data.keys() else ""
            self.AWS_SECRET_ACCESS_KEY = data["AWS_SECRET_ACCESS_KEY"] if "AWS_SECRET_ACCESS_KEY" in data.keys() else ""
            self.AWS_DEFAULT_REGION = data["AWS_DEFAULT_REGION"] if "AWS_DEFAULT_REGION" in data.keys() else "us-east-2"
            self.AWS_BUCKET = data["AWS_BUCKET"] if "AWS_BUCKET" in data.keys() else "gwtreasuremap"
            self.AZURE_ACCOUNT_NAME = data["AZURE_ACCOUNT_NAME"] if "AZURE_ACCOUNT_NAME" in data.keys() else ""
            self.AZURE_ACCOUNT_KEY = data["AZURE_ACCOUNT_KEY"] if "AZURE_ACCOUNT_KEY" in data.keys() else "" 
            self.STORAGE_BUCKET_SOURCE = data["STORAGE_BUCKET_SOURCE"] if "STORAGE_BUCKET_SOURCE" in data.keys() else "abfs"
            self.API_TOKEN = data["API_TOKEN"] if "API_TOKEN" in data.keys() else ""
            self.API_BASE = data["API_BASE"] if "API_BASE" in data.keys() else "http://127.0.0.1:5000/api/v0/"
            self.ALERT_DOMAIN = data["ALERT_DOMAIN"] if "ALERT_DOMAIN" in data.keys() else "igwn.gwalert"
            self.OBSERVING_RUN = data["OBSERVING_RUN"] if "OBSERVING_RUN" in data.keys() else "O4"
            self.KAFKA_CLIENT_ID = data["KAFKA_CLIENT_ID"] if "KAFKA_CLIENT_ID" in data.keys() else ""
            self.KAFKA_CLIENT_SECRET = data["KAFKA_CLIENT_SECRET"] if "KAFKA_CLIENT_SECRET" in data.keys() else ""
        else:
            self.AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', '')
            self.AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', '')
            self.AWS_DEFAULT_REGION = os.environ.get('AWS_DEFAULT_REGION', 'us-east-2')
            self.AWS_BUCKET = os.environ.get('AWS_BUCKET', 'gwtreasuremap')
            self.AZURE_ACCOUNT_NAME = os.environ.get('AZURE_ACCOUNT_NAME', '')
            self.AZURE_ACCOUNT_KEY = os.environ.get('AZURE_ACCOUNT_KEY', '')
            self.STORAGE_BUCKET_SOURCE = os.environ.get('STORAGE_BUCKET_SOURCE', 'abfs')
            self.API_TOKEN = os.environ.get('API_TOKEN', '')
            self.API_BASE = os.environ.get('API_BASE', 'http://127.0.0.1:5000/api/v0/')
            self.ALERT_DOMAIN = os.environ.get('ALERT_DOMAIN', 'igwn.gwalert')
            self.OBSERVING_RUN = os.environ.get('OBSERVING_RUN', 'O4')
            self.KAFKA_CLIENT_ID = os.environ.get('KAFKA_CLIENT_ID', '')
            self.KAFKA_CLIENT_SECRET = os.environ.get('KAFKA_CLIENT_SECRET', '')

