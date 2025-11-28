import os
import time
from datetime import datetime, timedelta, timezone

import requests


# === CONFIG YOU CAN SAFELY COMMIT (no secrets) ====================

# PurpleAir sensor IDs – you can later put multiple here and average
PURPLEAIR_SENSORS = [123421]  # Spruce Grove / Mom's sensor

# LIFX device ID (serial)
LIFX_DEVICE_ID = "d073d568e6e8"

# Duration for LIFX color fade
LIFX_DURATION_SEC = 60

# ================================================================

# Secrets come from environment (GitHub Actions secrets, NOT in repo)
PURPLEAIR_API_KEY = os.getenv("PURPLEAIR_API_KEY")
LIFX_API_KEY = os.getenv("LIFX_API_KEY")

if not PURPLEAIR_API_KEY:
    raise RuntimeError("PURPLEAIR_API_KEY is not set")
if not LIFX_API_KEY:
    raise RuntimeError("LIFX_API_KEY is not set")


def get_start_timestamp(minutes_back: int = 15) -> int:
    """
    Returns Unix epoch (int) for now - minutes_back, in UTC.
    PurpleAir history endpoint expects UTC timestamps.
    """
    now_utc = datetime.now(timezone.utc)
    start_time = now_utc - timedelta(minutes=minutes_back)
    return int(start_time.timestamp())


"""
pm_values = []
for sensor_id in PURPLEAIR_SENSORS:
    records = fetch_purpleair_history(sensor_id, start_ts)
    if not records:
        continue
    records_sorted = sorted(records, key=lambda r: r["time_stamp"], reverse=True)
    latest = records_sorted[0]
    pm_raw = latest.get("pm2.5_atm")
    rh = latest.get("humidity")
    if pm_raw is not None:
        pm_values.append(rh_correct_pm25(pm_raw, rh))

if not pm_values:
    print("No valid PM2.5 from any sensor; not changing light.")
    return

pm25_corr = sum(pm_values) / len(pm_values)
color = get_pa_color(pm25_corr)
"""



def fetch_purpleair_history(sensor_index: int, start_ts: int):
    """
    Call PurpleAir history endpoint for a single sensor.
    Returns a list of records (each is [time_stamp, humidity, pm2.5_atm]) or [].
    """
    base_url = f"https://api.purpleair.com/v1/sensors/{sensor_index}/history"

    params = {
        "average": 0,
        "start_timestamp": start_ts,
        # "humidity,pm2.5_atm" – PurpleAir will return these two columns
        "fields": "humidity,pm2.5_atm",
    }

    headers = {
        "X-API-Key": PURPLEAIR_API_KEY
    }

    resp = requests.get(base_url, params=params, headers=headers, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    # data["data"] is a list of rows; data["fields"] is list of column names
    fields = data.get("fields", [])
    rows = data.get("data", [])

    # Map into dicts for easier work: [{"time_stamp": ..., "humidity": ..., "pm2.5_atm": ...}, ...]
    results = []
    for row in rows:
        entry = {field: value for field, value in zip(fields, row)}
        results.append(entry)

    return results


def rh_correct_pm25(pm25_raw: float, rh: float) -> float:
    """
    Apply your RH correction logic from R.

    if RH < 30:
        denom = 1 + 0.24 / (100/30 - 1)
    elif 30 <= RH < 70:
        denom = 1 + 0.24 / (100/RH - 1)
    else (RH >= 70):
        denom = 1 + 0.24 / (100/70 - 1)
    """
    # Handle missing RH: default to 50%
    if rh is None:
        rh = 50.0

    try:
        rh = float(rh)
    except (TypeError, ValueError):
        rh = 50.0

    if rh < 30.0:
        denom = 1.0 + 0.24 / (100.0 / 30.0 - 1.0)
    elif rh < 70.0:
        denom = 1.0 + 0.24 / (100.0 / rh - 1.0)
    else:  # rh >= 70
        denom = 1.0 + 0.24 / (100.0 / 70.0 - 1.0)

    return float(pm25_raw) / denom


def get_pa_color(pm25_corr: float) -> str:
    """
    Port of your getPAColor() function.
    """
    try:
        v = float(pm25_corr)
    except (TypeError, ValueError):
        return "#D3D3D3"  # grey for NA / invalid

    if v > 100:
        return "#640100"
    elif v > 90:
        return "#9a0100"
    elif v > 80:
        return "#cc0001"
    elif v > 70:
        return "#fe0002"
    elif v > 60:
        return "#fd6866"
    elif v > 50:
        return "#ff9835"
    elif v > 40:
        return "#ffcb00"
    elif v > 30:
        return "#fffe03"
    elif v > 20:
        return "#016797"
    elif v > 10:
        return "#0099cb"
    else:
        return "#01cbff"


def set_lifx_color(color_hex: str):
    """
    Call LIFX HTTP API to set the bulb color.
    """
    url = f"https://api.lifx.com/v1/lights/id:{LIFX_DEVICE_ID}/state"
    headers = {
        "Authorization": f"Bearer {LIFX_API_KEY}",
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {
        "duration": LIFX_DURATION_SEC,
        "fast": False,
        "color": color_hex,
    }

    resp = requests.put(url, json=payload, headers=headers, timeout=20)
    if resp.status_code >= 400:
        raise RuntimeError(f"LIFX API error {resp.status_code}: {resp.text}")


def main():
    start_ts = get_start_timestamp(minutes_back=15)

    # For now, we just use the first sensor. Later you can loop and average.
    sensor_id = PURPLEAIR_SENSORS[0]

    records = fetch_purpleair_history(sensor_id, start_ts)

    if not records:
        print("No PurpleAir data returned; not changing light.")
        return

    # Sort by time_stamp descending and take the most recent
    records_sorted = sorted(records, key=lambda r: r["time_stamp"], reverse=True)
    latest = records_sorted[0]

    pm25_raw = latest.get("pm2.5_atm")
    rh = latest.get("humidity")

    if pm25_raw is None:
        print("Latest record had no pm2.5_atm; not changing light.")
        return

    pm25_corr = rh_correct_pm25(pm25_raw, rh)
    color = get_pa_color(pm25_corr)

    print(f"Latest PM2.5_atm={pm25_raw}, RH={rh}, corrected={pm25_corr:.2f}, color={color}")

    # Set the LIFX bulb color
    set_lifx_color(color)
    print("LIFX color updated.")


if __name__ == "__main__":
    main()
