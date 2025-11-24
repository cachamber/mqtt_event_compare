#!/usr/bin/env python3
"""MQTT event comparator.

Reads configuration from a JSON file and subscribes to a topic. For each
incoming event, it extracts a timestamp (from the payload if present, else
uses arrival time), compares with the previous event, and prints the two
timestamps and the differences between events.
"""
import argparse
import json
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Tuple

import paho.mqtt.client as mqtt


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_timestamp_from_value(v: Any) -> datetime:
    # v may be numeric epoch (seconds or ms) or an ISO string
    if v is None:
        raise ValueError("no timestamp")
    if isinstance(v, (int, float)):
        # Heuristics: if > 1e12 treat as ms
        if v > 1_000_000_000_000:
            return datetime.fromtimestamp(v / 1000.0, tz=timezone.utc)
        return datetime.fromtimestamp(v, tz=timezone.utc)
    if isinstance(v, str):
        s = v.strip()
        # numeric string?
        if s.isdigit():
            n = int(s)
            if n > 1_000_000_000_000:
                return datetime.fromtimestamp(n / 1000.0, tz=timezone.utc)
            return datetime.fromtimestamp(n, tz=timezone.utc)
        # try ISO formats
        try:
            # handle trailing Z
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            return datetime.fromisoformat(s)
        except Exception:
            raise
    raise ValueError("unsupported timestamp type")


def extract_timestamp(payload: Any) -> datetime:
    # payload may be dict (parsed JSON), bytes, or plain string
    if isinstance(payload, dict):
        for key in ("timestamp", "time", "ts", "t"):
            if key in payload:
                try:
                    return parse_timestamp_from_value(payload[key])
                except Exception:
                    pass
        # no obvious timestamp field, try top-level numeric fields
        for _k, v in payload.items():
            if isinstance(v, (int, float)) and v > 1_000_000_000:
                try:
                    return parse_timestamp_from_value(v)
                except Exception:
                    pass
    # if bytes or string that looks like a timestamp
    if isinstance(payload, (bytes, bytearray)):
        s = payload.decode(errors="ignore").strip()
    elif isinstance(payload, str):
        s = payload.strip()
    else:
        s = None
    if s:
        try:
            return parse_timestamp_from_value(s)
        except Exception:
            pass
    # fallback to arrival time
    return datetime.now(timezone.utc)


def diff_events(a: Any, b: Any, path: str = "") -> Dict[str, List[Tuple[str, Any]]]:
    """Return differences between a and b.

    Result keys: added (in b), removed (from a), changed (value changed).
    Each list holds tuples (path, value) for added/removed and
    (path, (old, new)) for changed. Paths are dotted for nested dicts
    and index-based for lists.
    """
    added = []
    removed = []
    changed = []

    def _path(p, key_name):
        return f"{p}.{key_name}" if p else str(key_name)

    if isinstance(a, dict) and isinstance(b, dict):
        keys = set(a.keys()) | set(b.keys())
        for key_name in sorted(keys):
            pa = a.get(key_name, None)
            pb = b.get(key_name, None)
            p = _path(path, key_name)
            if key_name not in a:
                added.append((p, pb))
            elif key_name not in b:
                removed.append((p, pa))
            else:
                if isinstance(pa, (dict, list)) and isinstance(pb, type(pa)):
                    sub = diff_events(pa, pb, p)
                    added.extend(sub["added"])
                    removed.extend(sub["removed"])
                    changed.extend(sub["changed"])
                else:
                    if pa != pb:
                        changed.append((p, (pa, pb)))
    elif isinstance(a, list) and isinstance(b, list):
        # compare by index
        n = max(len(a), len(b))
        for i in range(n):
            p = _path(path, f"[{i}]")
            ia = a[i] if i < len(a) else None
            ib = b[i] if i < len(b) else None
            if i >= len(a):
                added.append((p, ib))
            elif i >= len(b):
                removed.append((p, ia))
            else:
                if isinstance(ia, (dict, list)) and isinstance(ib, type(ia)):
                    sub = diff_events(ia, ib, p)
                    added.extend(sub["added"])
                    removed.extend(sub["removed"])
                    changed.extend(sub["changed"])
                else:
                    if ia != ib:
                        changed.append((p, (ia, ib)))
    else:
        if a != b:
            changed.append((path or "(value)", (a, b)))

    return {"added": added, "removed": removed, "changed": changed}


class MQTTComparator:
    def __init__(self, cfg: Dict[str, Any], debug: bool = False):
        self.cfg = cfg
        self.debug = debug
        self.client = mqtt.Client(client_id=cfg.get("client_id"))
        if cfg.get("username"):
            self.client.username_pw_set(cfg.get("username"), cfg.get("password"))
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.last_event = None
        self.last_payload = None
        self.last_ts = None
        # optional output file (path string) - either from config or None
        self.output_file = cfg.get("output_file")
        self._outfile = None
        if self.output_file:
            try:
                self._outfile = open(self.output_file, "a", encoding="utf-8")
            except Exception as e:
                print(f"Failed to open output file '{self.output_file}': {e}")

    def log(self, *args, **kwargs):
        if self.debug:
            print(*args, **kwargs)

    def write_line(self, *args, **kwargs):
        # Write a line to stdout and optionally to output file.
        sep = kwargs.pop("sep", " ")
        end = kwargs.pop("end", "\n")
        s = sep.join(str(a) for a in args) + end
        # stdout
        sys.stdout.write(s)
        sys.stdout.flush()
        # file
        if self._outfile:
            try:
                self._outfile.write(s)
                self._outfile.flush()
            except Exception:
                pass

    def on_connect(self, client, userdata, flags, rc, *args, **kwargs):
        if rc == 0:
            self.write_line(f"Connected to MQTT broker (rc={rc})")
            topic = self.cfg.get("topic")
            qos = int(self.cfg.get("qos", 0))
            self.write_line("Subscribing to topic:", topic, f"(qos={qos})")
            client.subscribe(topic, qos=qos)
        else:
            self.write_line(f"Failed to connect, rc={rc}")

    def on_message(self, client, userdata, msg, *args, **kwargs):
        payload_raw = msg.payload
        payload = None
        parsed = None
        try:
            s = payload_raw.decode("utf-8")
            try:
                parsed = json.loads(s)
                payload = parsed
            except Exception:
                payload = s
        except Exception:
            payload = payload_raw

        ts = extract_timestamp(parsed if parsed is not None else payload)
        if self.last_event is None:
            self.write_line("First event received:", ts.isoformat())
            self.last_event = payload
            self.last_payload = payload_raw
            self.last_ts = ts
            return

        self.write_line("---")
        self.write_line("Previous event timestamp:", self.last_ts.isoformat())
        self.write_line("Current event timestamp:", ts.isoformat())

        diff = diff_events(self.last_event, payload)

        if not (diff["added"] or diff["removed"] or diff["changed"]):
            self.write_line("No differences detected between events.")
        else:
            if diff["added"]:
                self.write_line("Added:")
                for p, v in diff["added"]:
                    self.write_line("  +", f"{p}:", v)
            if diff["removed"]:
                self.write_line("Removed:")
                for p, v in diff["removed"]:
                    self.write_line("  -", f"{p}:", v)
            if diff["changed"]:
                self.write_line("Changed:")
                for p, (old, new) in diff["changed"]:
                    self.write_line("  *", f"{p}:", f"{old} -> {new}")

        # update last
        self.last_event = payload
        self.last_payload = payload_raw
        self.last_ts = ts

    def run(self):
        host = self.cfg.get("host", "localhost")
        port = int(self.cfg.get("port", 1883))
        keepalive = int(self.cfg.get("keepalive", 60))
        if self.cfg.get("tls"):
            self.client.tls_set()
        self.write_line("Connecting to", f"{host}:{port}", f"(keepalive={keepalive})")
        self.client.connect(host, port, keepalive)
        try:
            self.client.loop_forever()
        except KeyboardInterrupt:
            self.write_line("Interrupted, exiting")
        finally:
            if self._outfile:
                try:
                    self._outfile.close()
                except Exception:
                    pass


def load_config(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def main(argv=None):
    p = argparse.ArgumentParser(description="MQTT event comparator")
    p.add_argument(
        "--config",
        "-c",
        default="config.json",
        help="Path to JSON config file",
    )
    p.add_argument(
        "--output",
        "-o",
        help=("Optional path to write output (overrides config output_file)"),
    )
    p.add_argument(
        "--debug",
        "-d",
        action="store_true",
        help="Enable debug output",
    )
    args = p.parse_args(argv)

    try:
        cfg = load_config(args.config)
    except Exception as e:
        print(f"Failed to load config '{args.config}': {e}")
        sys.exit(2)

    # allow CLI override for output file
    if args.output:
        cfg["output_file"] = args.output

    comparator = MQTTComparator(cfg, debug=args.debug)
    comparator.run()


if __name__ == "__main__":
    main()
