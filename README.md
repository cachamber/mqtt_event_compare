# MQTT Event Comparator

Small tool that subscribes to an MQTT topic and compares consecutive events.

Files created:
- `mqtt_compare.py` — main script
- `config.json.example` — example configuration
- `requirements.txt` — Python dependency list

Usage:

1. Install dependencies (use a venv if desired):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

2. Copy `config.json.example` to `config.json` and edit MQTT server and topic.

3. Run the comparator:

```bash
python3 mqtt_compare.py --config config.json
```

Options:
- `--config` / `-c`: Path to JSON config file (default `config.json`)
- `--debug` / `-d`: Enable debug logging

Config fields (JSON):
- `host`: MQTT broker host (default `localhost`)
- `port`: MQTT broker port (default `1883`)
- `topic`: Topic to subscribe to (required)
- `qos`: MQTT QoS (default `0`)
- `keepalive`: keepalive seconds (default `60`)
- `client_id`: optional client id
- `username`, `password`: optional credentials
- `tls`: boolean, whether to use TLS

Behavior:
- When the first event arrives the tool records it as the "previous" event.
- On each subsequent message it prints the timestamp of the previous and current
  event (tries to extract a `timestamp`, `time`, `ts`, or numeric epoch from
  the payload; falls back to arrival time), and lists added/removed/changed
  fields (recursive/dotted paths).

Docker
------

Build the Docker image:

```bash
docker build -t mqtt-event-compare .
```

Run the container (mount a config file):

```bash
docker run --rm -v "$PWD/config.json":/app/config.json mqtt-event-compare
```

To save output to a file inside the container, update `config.json` with an `output_file` path, or use `--output` when running the script.
