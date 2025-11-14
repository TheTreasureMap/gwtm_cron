FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

# Install system dependencies for SSL certificates
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create symlink for RedHat-style certificate path (required by librdkafka OIDC)
# librdkafka's OIDC token retrieval looks for /etc/pki/tls/certs/ca-bundle.crt
RUN mkdir -p /etc/pki/tls/certs && \
    ln -s /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt

RUN pip install --upgrade pip
COPY ./requirements.txt /app
RUN pip install -r requirements.txt

COPY . /app

# Install the gwtm_cron package
RUN pip install -e .

CMD ["python", "src/gwtm_cron/gwtm_listener/listener.py"]
