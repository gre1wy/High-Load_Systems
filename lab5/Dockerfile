FROM python:3.12-slim

WORKDIR /app

RUN pip install cassandra-driver

COPY main.py .

CMD ["python", "main.py"]