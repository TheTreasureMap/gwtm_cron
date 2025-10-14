FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

RUN pip install --upgrade pip
COPY ./requirements.txt /app
RUN pip install -r requirements.txt

COPY . /app

CMD ["python", "src/gwtm_cron/gwtm_listener/listener.py"]
