"""Fake band simulator: publishes vitals to the MQTT broker.

Usage: python simulator/band_sim.py [--host localhost] [--band band1] [--mode normal|tachy|hypoxia|noisy]
"""
import argparse
import json
import random
import time

import paho.mqtt.client as mqtt


def make_payload(mode: str) -> dict:
    p = {"hr": random.gauss(75, 4), "spo2": random.gauss(97, 1), "quality": 0.9, "contact": True, "battery": 85}
    if mode == "tachy":
        p["hr"] = random.gauss(150, 5)
    elif mode == "hypoxia":
        p["spo2"] = random.gauss(86, 1.5)
    elif mode == "noisy":
        p.update(hr=random.uniform(30, 220), quality=0.1)
    return p


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=1883)
    ap.add_argument("--band", default="band1")
    ap.add_argument("--mode", default="normal", choices=["normal", "tachy", "hypoxia", "noisy"])
    ap.add_argument("--interval", type=float, default=1.0)
    a = ap.parse_args()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect(a.host, a.port)
    client.loop_start()
    topic = f"zigbee2mqtt/{a.band}"
    print(f"publishing {a.mode} to {topic}")
    while True:
        client.publish(topic, json.dumps(make_payload(a.mode)))
        time.sleep(a.interval)


if __name__ == "__main__":
    main()
