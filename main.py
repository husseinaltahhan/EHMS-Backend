"""EHMS backend: band sends vitals over MQTT -> detector checks them -> dashboard gets them over WebSocket -> saved to Supabase."""
import asyncio
import json
import time
from contextlib import asynccontextmanager

import paho.mqtt.client as mqtt
from fastapi import FastAPI, WebSocket, WebSocketDisconnect

import db
from detector import Detector

# --- Settings ---
MQTT_HOST = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "zigbee2mqtt/+"  # + means "any band"

# --- State (lives in memory) ---
BAND_AGE_GROUPS = {"band1": "adult", 
                   "band2": "child",
                   "band3": "adult",
                   "band4": "elderly",
                   "band5": "adult"
                   }  # which age group each band's patient is in; unknown bands = adult
BAND_PATIENTS = {"band1": {"name": "John Doe", "sex": "Male", "age": 45}, 
                 "band2": {"name": "Mahir Jr", "sex": "Male", "age": 5},
                 "band3": {"name": "Peter Griffyn", "sex": "Male", "age": 35},
                 "band4": {"name": "Jane Doe", "sex": "Female", "age": 70},
                 "band5": {"name": "Miley Cirus", "sex": "Female", "age": 37}
                 }  # shown on the dashboard card
active_alerts = {}                    # (band_id, metric) -> alert
latest_samples = {}                   # band_id -> most recent sample, so new dashboards start filled in
dashboards = set()                    # open WebSocket connections
detector = Detector()


# --- Sending to the dashboard ---
async def broadcast(message):
    for ws in list(dashboards):
        try:
            await ws.send_json(message)
        except Exception:
            dashboards.discard(ws)


# --- The core flow: one reading in, alerts out ---
async def handle_sample(sample):
    sample["patient"] = BAND_PATIENTS.get(sample["band_id"])
    latest_samples[sample["band_id"]] = sample
    await broadcast({"type": "sample", "sample": sample})
    for event in detector.process(sample, BAND_AGE_GROUPS.get(sample["band_id"], "adult")):
        key = (event["alert"]["band_id"], event["alert"]["metric"])
        if event["type"] == "raised":
            active_alerts[key] = event["alert"]
        else:
            active_alerts.pop(key, None)
        print(f"ALERT {event['type']}: {event['alert']}")
        try:
            await asyncio.to_thread(db.save_events, sample, sample["patient"], event['type'], event['alert'])
        except Exception as e:
            print(f"couln't save event to db for {sample['band_id']}. Error: {e}")
        await broadcast(event)
    # Save to Supabase last, so a slow or offline database never delays the dashboard.
    try:
        await asyncio.to_thread(db.save_vitals, sample, sample["patient"])
    except Exception as e:
        print(f"couldn't save vitals for {sample['band_id']}: {e}")


# --- Listening to the bands over MQTT ---
def parse_payload(topic, payload):
    """Turns an MQTT message into a sample dict. Change this when the real band payload is known."""
    data = json.loads(payload)
    data["band_id"] = topic.split("/")[-1]  # zigbee2mqtt/band1 -> band1
    data.setdefault("ts", time.time())
    return data


def start_mqtt(loop):
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)

    def on_connect(c, userdata, flags, reason_code, properties):
        print(f"MQTT connected, listening on {MQTT_TOPIC}")
        c.subscribe(MQTT_TOPIC)

    def on_message(c, userdata, msg):
        try:
            sample = parse_payload(msg.topic, msg.payload)
        except Exception as e:
            print(f"bad message on {msg.topic}: {e}")
            return
        # paho runs on its own thread; this hands the sample over to FastAPI's thread.
        asyncio.run_coroutine_threadsafe(handle_sample(sample), loop)

    client.on_connect = on_connect
    client.on_message = on_message
    client.connect_async(MQTT_HOST, MQTT_PORT)
    client.loop_start()
    return client


# --- Web server ---
@asynccontextmanager
async def lifespan(app):
    client = start_mqtt(asyncio.get_running_loop())  # runs when the server starts
    yield
    client.loop_stop()                                # runs when the server stops


app = FastAPI(title="EHMS Backend", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/alerts")
async def list_alerts():
    return list(active_alerts.values())


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    await ws.accept()
    # Catch the new dashboard up on everything it missed before it connected.
    await ws.send_json({"type": "snapshot",
                        "samples": list(latest_samples.values()),
                        "alerts": list(active_alerts.values())})
    dashboards.add(ws)
    try:
        while True:
            await ws.receive_text()  # keeps the connection open
    except WebSocketDisconnect:
        dashboards.discard(ws)
