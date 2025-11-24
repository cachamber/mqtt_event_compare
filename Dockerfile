FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY mqtt_compare.py ./
COPY config.json.example ./config.json

CMD ["python", "mqtt_compare.py", "--config", "config.json"]
