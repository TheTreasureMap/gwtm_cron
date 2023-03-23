FROM python:3.10

WORKDIR /app

ENV PYTHONBUFFERED 1

RUN pip install --upgrade pip
COPY ./requirements.txt /app
RUN pip install -r requirements.txt

COPY . /app

CMD ["python", "src/gwtm_cron/gwtm_listener/listener.py"]
