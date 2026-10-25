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