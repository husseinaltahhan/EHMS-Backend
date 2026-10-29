"""Saving to Supabase: each band reading becomes a row in the vitals table. No MQTT or web code here."""
import os
import re
import threading
from datetime import datetime, timezone

from dotenv import load_dotenv
from supabase import create_client

load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")  # the secret key; never put it in the frontend

client = create_client(SUPABASE_URL, SUPABASE_KEY) if SUPABASE_URL and SUPABASE_KEY else None
session_ids = {}                # band_id -> id of its active row in the sessions table
session_lock = threading.Lock() # stops two readings from the same band creating two sessions


def to_iso(ts):
    return datetime.fromtimestamp(ts, timezone.utc).isoformat()


def to_int(value):
    return None if value is None else round(value)


def band_number(band_id):
    """bands.id is a number, so zigbee2mqtt/band1 -> 1. Returns None if the name has no number."""
    match = re.search(r"\d+$", band_id)
    return int(match.group()) if match else None


def get_session_id(sample, patient):
    """Finds the band's active session, or opens one using the patient's details."""
    band_id = sample["band_id"]
    with session_lock:
        if band_id in session_ids:
            return session_ids[band_id]

        number = band_number(band_id)
        if number is None:
            return None

        found = (client.table("sessions").select("id")
                 .eq("band_id", number).eq("active", True)
                 .order("created_at", desc=True).limit(1).execute().data)
        if found:
            session_ids[band_id] = found[0]["id"]
        elif patient:
            # Sessions need a row in bands first (foreign key).
            client.table("bands").upsert({"id": number, "current_charge": to_int(sample.get("battery")) or 0,
                                          "last_updated": to_iso(sample["ts"]), "active": True}).execute()
            created = client.table("sessions").insert({
                "band_id": number, "active": True, "patient_name": patient["name"],
                "patient_age": patient["age"], "patient_sex": patient["sex"],
            }).execute().data
            session_ids[band_id] = created[0]["id"]
            print(f"Opened session {created[0]['id']} for {band_id} ({patient['name']})")
        else:
            return None  # unknown band and nobody admitted on it: nothing to attach the reading to
        return session_ids[band_id]


def save_vitals(sample, patient):
    """Writes one reading to the vitals table. Blocking, so call it with asyncio.to_thread."""
    if client is None:
        return
    session_id = get_session_id(sample, patient)
    if session_id is None:
        return
    client.table("vitals").insert({
        "session_id": session_id,
        "heart_rate": to_int(sample.get("hr")),
        "oxygen_sat": to_int(sample.get("spo2")),
        "body_temp": to_int(sample.get("temp")),
        "ecg": to_int(sample.get("ecg")),
        "created_at": to_iso(sample["ts"]),
    }).execute()


def save_events(sample, patient, alert_type, alert_desc):
    if client is None:
        return
    session_id = get_session_id(sample, patient)
    if session_id is None:
        return
    client.table("events").insert({
        "name" : alert_type,
        "description" : alert_desc,
        "session_id" : session_id,
    }).execute()

def save_alert(sample, patient, event):
    """Raised -> new row in alerts marked active. Cleared -> that row is marked resolved."""
    if client is None:
        return
    session_id = get_session_id(sample, patient)
    if session_id is None:
        return
    alert = event["alert"]
    if event["type"] == "raised":
        client.table("alerts").insert({
            "session_id": session_id,
            "type": alert["metric"],  # hr_high, hr_low or spo2_low
            "status": "active",
            "created_at": to_iso(alert["raised_at"]),
        }).execute()
    else:
        # Matched by session and type rather than a saved id, so it still works after a backend restart.
        (client.table("alerts").update({"status": "resolved", "resolved_at": to_iso(sample["ts"])})
         .eq("session_id", session_id).eq("type", alert["metric"]).eq("status", "active").execute())


def get_sessions():
    if client is None:
        return
    return client.table("sessions").select("*").eq("active", True).execute().data


class BandInUse(Exception):
    """Raised when admitting a patient onto a band that's already on someone."""


def create_session(details):
    """Admits a patient: opens a session on their band. details has the sessions columns (band_id, patient_name, ...)."""
    if client is None:
        raise RuntimeError("Supabase isn't configured (check .env)")
    number = details["band_id"]
    with session_lock:
        active = client.table("sessions").select("id, patient_name").eq("band_id", number).eq("active", True).execute().data
        if active:
            raise BandInUse(f"Band {number} is already on {active[0]['patient_name']} (session {active[0]['id']})")

        # Sessions need a row in bands first (foreign key). ignore_duplicates leaves an existing band untouched.
        client.table("bands").upsert({"id": number, "current_charge": 0, "active": True},
                                     ignore_duplicates=True).execute()
        session = client.table("sessions").insert({**details, "active": True}).execute().data[0]

        # Point this band's live readings at the new session.
        for band_id in [b for b in session_ids if band_number(b) == number]:
            session_ids[band_id] = session["id"]
        return session
