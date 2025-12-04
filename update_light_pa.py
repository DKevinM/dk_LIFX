import os
import time
import json
import math
import csv
from datetime import datetime, timedelta, timezone

import requests
import argparse


# === CONFIG YOU CAN SAFELY COMMIT (no secrets) ====================

# PurpleAir sensor IDs – you can later put multiple here and average
# PURPLEAIR_SENSORS = [123421]  # Spruce Grove / Mom's sensor test case
PURPLEAIR_SENSORS = [166965,83971,91545]  # Evansburg / Entwistle
# LIFX device ID (serial)
LIFX_DEVICE_ID = "d073d568e6e8"

# Duration for LIFX color fade
LIFX_DURATION_SEC = 60

# Output JSON file (for map / phone app)
STATUS_JSON_PATH = os.path.join("data", "purpleair_light_status.json")

# Consider data "fresh" if last_seen is within this many minutes
MAX_AGE_MINUTES = 30

# ================================================================

# Secrets come from environment (GitHub Actions secrets, NOT in repo)
PURPLEAIR_API_KEY = os.getenv("PURPLEAIR_API_KEY")
LIFX_API_KEY = os.getenv("LIFX_API_KEY")

if not PURPLEAIR_API_KEY:
    raise RuntimeError("PURPLEAIR_API_KEY is not set")
if not LIFX_API_KEY:
    raise RuntimeError("LIFX_API_KEY is not set")


# ---------- PurpleAir helper logic --------------------------------

def _is_na(x):
    """Minimal 'is.na' equivalent without pandas."""
    if x is None:
        return True
    if isinstance(x, float) and math.isnan(x):
        return True
    return False


# Robust PM2.5 calculation (R logic ported) - ADD THESE FUNCTIONS
def get_best_pm(a, b, avg):
    """
    Robust PM2.5 calculation (R logic ported).

    a   = pm2.5_atm_a
    b   = pm2.5_atm_b
    avg = pm2.5_atm (PurpleAir's own average)
    """
    # Handle extreme / missing cases
    if _is_na(a) and not _is_na(b) and b <= 2000:
        return b
    if _is_na(b) and not _is_na(a) and a <= 2000:
        return a
    if not _is_na(a) and a > 2000 and not _is_na(b) and b <= 2000:
        return b
    if not _is_na(b) and b > 2000 and not _is_na(a) and a <= 2000:
        return a

    if not _is_na(a) and not _is_na(b):
        diff = abs(a - b)
        if diff > 50 and diff <= 500:
            return max(a, b)
        elif diff > 500:
            return None
        elif diff <= 50 and not _is_na(avg) and avg >= 0:
            return avg

    # Fallback
    return avg



def rh_correct_pm25(pm25_raw: float, rh: float) -> float:
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

    if v > 100: return "#640100"  #eAQHI 10+
    elif v > 90: return "#9a0100" #eAQHI 10
    elif v > 80: return "#cc0001" #eAQHI 9
    elif v > 70: return "#fe0002" #eAQHI 8
    elif v > 60: return "#fd6866" #eAQHI 7
    elif v > 50: return "#ff9835" #eAQHI 6
    elif v > 40: return "#ffcb00" #eAQHI 5
    elif v > 30: return "#fffe03" #eAQHI 4
    elif v > 20: return "#016797" #eAQHI 3
    elif v > 10: return "#0099cb" #eAQHI 2
    elif v > 0: return "#01cbff"  #eAQHI 1
    else: return "#D3D3D3"



def _safe_float(x):
    try:
        if x is None:
            return None
        s = str(x).strip()
        if not s:
            return None
        return float(s)
    except (TypeError, ValueError):
        return None



def load_sensor_metadata(sensor_ids):
    """
    Load metadata (name, lat, lon, geometry) from a CSV hosted on GitHub.

    Expects a CSV with at least:
      sensor_index, name, latitude, longitude, geometry

    Only returns rows whose sensor_index is in sensor_ids.
    """
    url = os.getenv("PA_SENSORS_CSV_URL")
    if not url:
        print("PA_SENSORS_CSV_URL not set; skipping metadata load.")
        return {}

    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        print(f"Warning: could not fetch sensor metadata CSV: {e}")
        return {}

    lines = resp.text.splitlines()
    reader = csv.DictReader(lines)

    # Normalise sensor_ids to ints for matching
    id_set = set()
    for sid in sensor_ids:
        try:
            id_set.add(int(sid))
        except (TypeError, ValueError):
            pass

    meta = {}
    for row in reader:
        raw_id = row.get("sensor_index") or row.get("SensorIndex") or row.get("id")
        try:
            sid = int(str(raw_id).strip())
        except (TypeError, ValueError):
            continue

        if sid not in id_set:
            continue

        name = row.get("name") or row.get("Name")
        lat = _safe_float(row.get("latitude") or row.get("lat") or row.get("Latitude"))
        lon = _safe_float(row.get("longitude") or row.get("lon") or row.get("Longitude"))
        geom = row.get("geometry") or row.get("wkt") or row.get("geom")

        meta[sid] = {
            "name": name,
            "latitude": lat,
            "longitude": lon,
            "geometry": geom,
        }

    print(f"Loaded metadata for {len(meta)} sensors from CSV.")
    return meta




def fetch_purpleair_current_multi(sensor_ids, max_age_minutes=30):
    """
    Call PurpleAir /v1/sensors once for all sensor_ids using show_only.

    Returns a list of dicts, one per sensor, each like:
      {
        "sensor_index": int,
        "last_seen": int or None,
        "last_seen_iso_utc": str or None,
        "humidity": float or None,
        "pm25_atm": float or None,
        "pm25_atm_a": float or None,
        "pm25_atm_b": float or None,
        "pm25_best": float or None,
        "pm25_corr": float or None,
        "is_fresh": bool
      }
    """
    if not sensor_ids:
        return []

    sensor_id_str = ",".join(str(s) for s in sensor_ids)

    url = "https://api.purpleair.com/v1/sensors"
    headers = {"X-API-Key": PURPLEAIR_API_KEY}
    params = {
        "fields": "sensor_index,last_seen,humidity,pm2.5_atm,pm2.5_atm_a,pm2.5_atm_b",
        "show_only": sensor_id_str,
    }

    resp = requests.get(url, headers=headers, params=params, timeout=20)
    resp.raise_for_status()
    data = resp.json()

    fields = data.get("fields", [])
    rows = data.get("data", [])

    now_ts = time.time()
    max_age_sec = max_age_minutes * 60

    results = []
    for row in rows:
        entry = {field: value for field, value in zip(fields, row)}

        sid = entry.get("sensor_index")
        last_seen = entry.get("last_seen")
        rh = entry.get("humidity")
        pm_atm = entry.get("pm2.5_atm")
        pm_a = entry.get("pm2.5_atm_a")
        pm_b = entry.get("pm2.5_atm_b")

        # Determine freshness
        if isinstance(last_seen, (int, float)):
            age_sec = now_ts - last_seen
            is_fresh = age_sec <= max_age_sec
            ts_iso = datetime.fromtimestamp(last_seen, tz=timezone.utc).isoformat()
        else:
            is_fresh = False
            ts_iso = None

        # Robust PM selection
        best_pm = get_best_pm(pm_a, pm_b, pm_atm)

        # RH correction only if data is fresh and best_pm is valid
        if is_fresh and best_pm is not None and not _is_na(best_pm):
            pm_corr = rh_correct_pm25(best_pm, rh)
        else:
            pm_corr = None

        results.append(
            {
                "sensor_index": sid,
                "last_seen": last_seen,
                "last_seen_iso_utc": ts_iso,
                "humidity": rh,
                "pm25_atm": pm_atm,
                "pm25_atm_a": pm_a,
                "pm25_atm_b": pm_b,
                "pm25_best": best_pm,
                "pm25_corr": pm_corr,
                "is_fresh": is_fresh,
            }
        )

    return results



# ---------- LIFX + CLI helpers -----------------------------------

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



def manual_override():
    parser = argparse.ArgumentParser()
    parser.add_argument("--color", help="Manually set LIFX bulb color (e.g., #FF0000)")
    args = parser.parse_args()

    if args.color:
        print(f"Manual override: setting color to {args.color}")
        set_lifx_color(args.color)
        print("Manual LIFX color update complete.")
        return True
    return False


# ---------- JSON status helpers ----------------------------------


def build_status_payload(
    sensors_data,
    used_sensor_indices,
    used_pm25_corr,
    used_color_hex,
    strategy: str,
):
    """
    Build a JSON-serializable dict describing the current status.
    sensors_data: list of dicts from fetch_purpleair_current_multi().
    used_sensor_indices: list of sensor_index values that contributed to the light color
    strategy: e.g. "average_fresh_sensors" or "none_available"
    """
    now_utc = datetime.now(timezone.utc).isoformat()

    payload = {
        "generated_at_utc": now_utc,
        "sensors": sensors_data,
        "light": {
            "lifx_device_id": LIFX_DEVICE_ID,
            "strategy": strategy,
            "used_sensor_indices": used_sensor_indices,
            "used_pm25_corr": used_pm25_corr,
            "color_hex": used_color_hex,
            "duration_sec": LIFX_DURATION_SEC,
        },
    }
    return payload



def write_status_json(payload, path: str = STATUS_JSON_PATH):
    """
    Write the status payload to a JSON file.
    """
    try:
        # Ensure parent directory exists (e.g., data/)
        dir_name = os.path.dirname(path)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
        print(f"Wrote status JSON to {path}")
    except Exception as e:
        # Don't kill the run if JSON write fails – just log it.
        print(f"Warning: failed to write status JSON: {e}")



# ---------- MAIN -------------------------------------------------

def main():
    # 1) Fetch data for all configured sensors via /v1/sensors + show_only
    sensors_status = fetch_purpleair_current_multi(
        PURPLEAIR_SENSORS, max_age_minutes=MAX_AGE_MINUTES
    )
    
    # Merge in lat/lon/name/geometry from AB_PA_sensors.csv (if available)
    sensor_ids_for_meta = [
        s.get("sensor_index") for s in sensors_status if s.get("sensor_index") is not None
    ]
    meta_by_id = load_sensor_metadata(sensor_ids_for_meta)

    for s in sensors_status:
        sid = s.get("sensor_index")
        if sid in meta_by_id:
            s.update(meta_by_id[sid])


    if not PURPLEAIR_SENSORS:
        print("No PurpleAir sensors configured; not changing light.")
        payload = build_status_payload(
            sensors_data=sensors_status,
            used_sensor_indices=[],
            used_pm25_corr=None,
            used_color_hex=None,
            strategy="no_sensors_configured",
        )
        write_status_json(payload)
        return


    # 2) Select all fresh sensors with a valid corrected PM value
    usable = [
        s for s in sensors_status
        if s.get("is_fresh") and s.get("pm25_corr") is not None
    ]

    if not usable:
        print("No fresh valid PurpleAir data; not changing light.")
        payload = build_status_payload(
            sensors_data=sensors_status,
            used_sensor_indices=[],
            used_pm25_corr=None,
            used_color_hex=None,
            strategy="none_available",
        )
        write_status_json(payload)
        return

    used_sensor_indices = [
        int(s["sensor_index"]) for s in usable
        if s.get("sensor_index") is not None
    ]
    pm_vals = [float(s["pm25_corr"]) for s in usable]
    avg_pm25_corr = sum(pm_vals) / len(pm_vals)

    color = get_pa_color(avg_pm25_corr)

    print(
        f"Using {len(usable)} sensors {used_sensor_indices}: "
        f"avg_corrected={avg_pm25_corr:.2f}, color={color}"
    )
  

    # 3) Set the LIFX bulb color
    set_lifx_color(color)
    print("LIFX color updated.")

    # 4) Write status JSON for mapping / phone use
    payload = build_status_payload(
        sensors_data=sensors_status,
        used_sensor_indices=used_sensor_indices,
        used_pm25_corr=avg_pm25_corr,
        used_color_hex=color,
        strategy="average_fresh_sensors",
    )
    write_status_json(payload)


if __name__ == "__main__":
    # If manual color was supplied, handle that and exit.
    if manual_override():
        exit(0)

    # Otherwise run normal PurpleAir logic
    main()
