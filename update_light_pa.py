import os
import time
import json
import math
import csv
import colorsys
from datetime import datetime, timedelta, timezone

import requests
import argparse
import pandas as pd


# === CONFIG YOU CAN SAFELY COMMIT (no secrets) ====================

# PurpleAir sensor IDs – you can later put multiple here and average
# PURPLEAIR_SENSORS = [123421]  # Spruce Grove / Mom's sensor test case
PURPLEAIR_SENSORS = [166965,83971,91545,249949]  # Evansburg / Entwistle
# LIFX device ID (serial)
# LIFX_DEVICE_ID = "d073d568e6e8"
LIFX_DEVICE_ID = "D073D5D54604"

# Duration for LIFX color fade
LIFX_DURATION_SEC = 60

# Output JSON file (for map / phone app)
STATUS_JSON_PATH = os.path.join("data", "purpleair_light_status.json")

# Running estimate-vs-official AQHI comparison log (appended to every run)
COMPARISON_LOG_PATH = os.path.join("data", "aqhi_comparison_log.csv")

# Source of official station AQHI readings (same feed index.html's map compares against)
AQHI_STATIONS_CSV_URL = "https://raw.githubusercontent.com/DKevinM/AB_datapull/main/data/last6h.csv"

# Consider data "fresh" if last_seen is within this many minutes
MAX_AGE_MINUTES = 30

# Estimate-vs-official agreement thresholds, in AQHI category points.
# |diff| <= HIGH  -> "high" confidence (estimate and official are in/near the same band)
# |diff| <= MEDIUM -> "medium" confidence
# otherwise         -> "low" confidence (system is telling you it disagrees with itself)
CONFIDENCE_HIGH_MAX_DIFF = 1
CONFIDENCE_MEDIUM_MAX_DIFF = 2

# When confidence is "low", dim/desaturate the displayed color instead of
# showing it at full strength (rather than blinking, which can read as an
# alarm). These are starting points — tune once seen in person.
LOW_CONFIDENCE_SATURATION_FACTOR = 0.35
LOW_CONFIDENCE_BRIGHTNESS_FACTOR = 0.55
LOW_CONFIDENCE_MIN_BRIGHTNESS = 0.15

# ================================================================

# Secrets come from environment (GitHub Actions secrets, NOT in repo)
PURPLEAIR_API_KEY = os.getenv("PURPLEAIR_API_KEY")
LIFX_API_KEY = os.getenv("LIFX_API_KEY")

if not PURPLEAIR_API_KEY:
    raise RuntimeError("PURPLEAIR_API_KEY is not set")
if not LIFX_API_KEY:
    raise RuntimeError("LIFX_API_KEY is not set")


# ---------- PurpleAir helper logic --------------------------------

def choose_pm_and_method(a, b, avg, forced=None):
    # forced can be "A", "B", "OFF", or None
    if forced == "OFF":
        return None, "off"
    if forced == "A":
        return a, "forced_A"
    if forced == "B":
        return b, "forced_B"

    # hard invalids
    if _is_na(a) and not _is_na(b) and b <= 2000:
        return b, "b_only"
    if _is_na(b) and not _is_na(a) and a <= 2000:
        return a, "a_only"
    if not _is_na(a) and a > 2000 and not _is_na(b) and b <= 2000:
        return b, "b_only_a_spike"
    if not _is_na(b) and b > 2000 and not _is_na(a) and a <= 2000:
        return a, "a_only_b_spike"

    if not _is_na(a) and not _is_na(b):
        diff = abs(a - b)
        if diff > 500:
            return None, "extreme_diff_reject"
        if diff > 50:
            if max(a, b) < 50:   # low concentrations → noise dominates
                return min(a, b), "min_low_range"
            else:
                return max(a, b), "max_high_range"
        if not _is_na(avg) and 0 <= avg <= 2500:
            return avg, "avg"

    return avg, "fallback_avg"


def _is_na(x):
    """Minimal 'is.na' equivalent without pandas."""
    if x is None:
        return True
    if isinstance(x, float) and math.isnan(x):
        return True
    return False


def load_channel_override_local(path="data/channel_override.csv"):
    try:
        df = pd.read_csv(path)
        df["sensor_index"] = df["sensor_index"].astype(int)
        return dict(zip(df["sensor_index"], df["force_channel"]))
    except Exception:
        return {}



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





# ---------- Official AQHI comparison (ported from index.html) -----

def fetch_aqhi_stations(url=AQHI_STATIONS_CSV_URL):
    """
    Pull the latest official AQHI reading per station from the same
    last6h.csv feed the map page (index.html) compares against.

    Rows in that feed use a blank ParameterName to mean "this row is AQHI"
    (see index.html's fetchAQHIStations for the original JS version).
    """
    try:
        resp = requests.get(url, timeout=20)
        resp.raise_for_status()
    except Exception as e:
        print(f"Warning: could not fetch AQHI stations CSV: {e}")
        return []

    reader = csv.DictReader(resp.text.splitlines())

    latest_by_station = {}
    for row in reader:
        param = (row.get("ParameterName") or "").strip()
        if param != "":
            continue  # AQHI rows only

        station = row.get("StationName")
        val = _safe_float(row.get("Value"))
        lat = _safe_float(row.get("Latitude"))
        lon = _safe_float(row.get("Longitude"))
        date_str = row.get("ReadingDate")

        if not station or val is None or lat is None or lon is None:
            continue

        try:
            ts = datetime.fromisoformat(date_str)
        except (TypeError, ValueError):
            continue

        existing = latest_by_station.get(station)
        if existing is None or ts > existing["time"]:
            latest_by_station[station] = {
                "station": station,
                "lat": lat,
                "lon": lon,
                "value": val,
                "time": ts,
            }

    return list(latest_by_station.values())


def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def get_three_closest_aqhi(stations, lat, lon):
    """
    Distance-weighted (inverse-distance) average AQHI across the 3 closest
    official stations, rounded up — matches getThreeClosestAQHI() in index.html.
    """
    if lat is None or lon is None or not stations:
        return None

    with_dist = [
        {**s, "distance": haversine_km(lat, lon, s["lat"], s["lon"])}
        for s in stations
    ]
    with_dist.sort(key=lambda s: s["distance"])
    closest3 = with_dist[:3]
    if not closest3:
        return None

    weighted_sum = 0.0
    weight_total = 0.0
    for s in closest3:
        w = 1.0 / max(s["distance"], 0.1)
        weighted_sum += s["value"] * w
        weight_total += w

    avg = weighted_sum / weight_total
    return {"stations": closest3, "avg": avg, "rounded": math.ceil(avg)}


def estimate_aqhi_from_pm25(pm25_corr):
    """Mirrors index.html's estAQHI: floor(avgPm / 10) + 1 (uncapped here)."""
    return math.floor(pm25_corr / 10) + 1


def classify_confidence(estimated_aqhi, official_rounded):
    """
    Confidence that the displayed signal reflects reality, based on how far the
    PurpleAir-derived estimate diverges from the 3-closest-station official AQHI.

    Returns "unknown" when there's nothing to compare against (no nearby
    official stations, or no site location) rather than defaulting to "high" —
    absence of a check is not the same as agreement.
    """
    if estimated_aqhi is None or official_rounded is None:
        return "unknown"

    diff = abs(estimated_aqhi - official_rounded)
    if diff <= CONFIDENCE_HIGH_MAX_DIFF:
        return "high"
    if diff <= CONFIDENCE_MEDIUM_MAX_DIFF:
        return "medium"
    return "low"


def hex_to_hsb(hex_color):
    h = hex_color.lstrip("#")
    r, g, b = (int(h[i:i + 2], 16) / 255.0 for i in (0, 2, 4))
    hue, sat, val = colorsys.rgb_to_hsv(r, g, b)
    return hue * 360.0, sat, val


def apply_confidence_dimming(color_hex, confidence):
    """
    When the PurpleAir-derived estimate disagrees with the 3-closest official
    stations (confidence == "low"), dim and desaturate the eAQHI category
    color instead of displaying it at full strength — a physical "I'm not
    sure about this" cue on a bulb that only has one color to give. Color is
    left untouched for "high"/"medium"/"unknown" confidence.

    Returns a LIFX structured color string ("hue:.. saturation:.. brightness:..")
    when dimming is applied, or the original hex string unchanged otherwise.
    """
    if confidence != "low":
        return color_hex

    hue, sat, val = hex_to_hsb(color_hex)
    new_sat = max(0.0, sat * LOW_CONFIDENCE_SATURATION_FACTOR)
    new_val = max(LOW_CONFIDENCE_MIN_BRIGHTNESS, val * LOW_CONFIDENCE_BRIGHTNESS_FACTOR)
    return f"hue:{hue:.1f} saturation:{new_sat:.3f} brightness:{new_val:.3f}"


def compute_site_centroid(sensors):
    """Average lat/lon of the sensors actually used, as a stand-in site location."""
    lats = [s.get("latitude") for s in sensors if s.get("latitude") is not None]
    lons = [s.get("longitude") for s in sensors if s.get("longitude") is not None]
    if not lats or not lons:
        return None, None
    return sum(lats) / len(lats), sum(lons) / len(lons)


COMPARISON_LOG_FIELDS = [
    "timestamp_utc",
    "n_sensors_used",
    "used_sensor_indices",
    "pm25_corr_avg",
    "estimated_aqhi",
    "site_lat",
    "site_lon",
    "station1_name", "station1_km", "station1_aqhi",
    "station2_name", "station2_km", "station2_aqhi",
    "station3_name", "station3_km", "station3_aqhi",
    "official_weighted_avg",
    "official_rounded",
    "diff_estimated_minus_official",
    "confidence",
]


def append_comparison_row(row, path=COMPARISON_LOG_PATH):
    """Append one row to the running comparison CSV, writing the header once."""
    dir_name = os.path.dirname(path)
    if dir_name:
        os.makedirs(dir_name, exist_ok=True)

    file_exists = os.path.isfile(path)
    try:
        with open(path, "a", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=COMPARISON_LOG_FIELDS)
            if not file_exists:
                writer.writeheader()
            writer.writerow(row)
        print(f"Appended comparison row to {path}")
    except Exception as e:
        print(f"Warning: failed to append comparison row: {e}")


def build_comparison_row(usable, used_sensor_indices, avg_pm25_corr):
    site_lat, site_lon = compute_site_centroid(usable)
    estimated_aqhi = estimate_aqhi_from_pm25(avg_pm25_corr)

    aqhi_stations = fetch_aqhi_stations()
    aqhi_compare = get_three_closest_aqhi(aqhi_stations, site_lat, site_lon)

    row = {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "n_sensors_used": len(usable),
        "used_sensor_indices": ";".join(str(i) for i in used_sensor_indices),
        "pm25_corr_avg": round(avg_pm25_corr, 2),
        "estimated_aqhi": estimated_aqhi,
        "site_lat": site_lat,
        "site_lon": site_lon,
    }

    for i in range(3):
        prefix = f"station{i + 1}"
        if aqhi_compare and i < len(aqhi_compare["stations"]):
            s = aqhi_compare["stations"][i]
            row[f"{prefix}_name"] = s["station"]
            row[f"{prefix}_km"] = round(s["distance"], 2)
            row[f"{prefix}_aqhi"] = s["value"]
        else:
            row[f"{prefix}_name"] = None
            row[f"{prefix}_km"] = None
            row[f"{prefix}_aqhi"] = None

    if aqhi_compare:
        row["official_weighted_avg"] = round(aqhi_compare["avg"], 2)
        row["official_rounded"] = aqhi_compare["rounded"]
        row["diff_estimated_minus_official"] = estimated_aqhi - aqhi_compare["rounded"]
        row["confidence"] = classify_confidence(estimated_aqhi, aqhi_compare["rounded"])
    else:
        row["official_weighted_avg"] = None
        row["official_rounded"] = None
        row["diff_estimated_minus_official"] = None
        row["confidence"] = "unknown"

    return row


def fetch_purpleair_current_multi(sensor_ids, overrides, max_age_minutes=30):
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
        # Optional channel override support
        
        forced = overrides.get(int(sid)) if sid is not None else None
        
        best_pm, pm_method = choose_pm_and_method(
            pm_a,
            pm_b,
            pm_atm,
            forced=forced
        )

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
                "pm_method": pm_method,
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
    comparison=None,
):
    """
    Build a JSON-serializable dict describing the current status.
    sensors_data: list of dicts from fetch_purpleair_current_multi().
    used_sensor_indices: list of sensor_index values that contributed to the light color
    strategy: e.g. "average_fresh_sensors" or "none_available"
    comparison: optional row from build_comparison_row(), surfaces estimate-vs-official
        agreement (and "confidence") to consumers of the JSON, e.g. the map page.
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
        "comparison": comparison,
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
    overrides = load_channel_override_local()
    
    sensors_status = fetch_purpleair_current_multi(
        PURPLEAIR_SENSORS,
        overrides,
        max_age_minutes=MAX_AGE_MINUTES
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
        stale_color = "#D3D3D3"  # grey = data stale/unavailable, not "safe"
        print("No fresh valid PurpleAir data; setting light to grey (stale).")
        try:
            set_lifx_color(stale_color)
        except Exception as e:
            print(f"Warning: failed to set stale-data fallback color: {e}")

        payload = build_status_payload(
            sensors_data=sensors_status,
            used_sensor_indices=[],
            used_pm25_corr=None,
            used_color_hex=stale_color,
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

    # 2b) Compare against the 3 closest official AQHI stations and log it
    comparison_row = build_comparison_row(usable, used_sensor_indices, avg_pm25_corr)
    append_comparison_row(comparison_row)
    print(
        f"Estimated AQHI={comparison_row['estimated_aqhi']} vs "
        f"official (3-closest, weighted)={comparison_row['official_rounded']} "
        f"-> confidence={comparison_row['confidence']}"
    )

    # 3) Set the LIFX bulb color — dimmed/desaturated if confidence is low
    lifx_command_color = apply_confidence_dimming(color, comparison_row["confidence"])
    if lifx_command_color != color:
        print(f"Low confidence: dimming bulb command to '{lifx_command_color}'")
    set_lifx_color(lifx_command_color)
    print("LIFX color updated.")

    # 4) Write status JSON for mapping / phone use
    payload = build_status_payload(
        sensors_data=sensors_status,
        used_sensor_indices=used_sensor_indices,
        used_pm25_corr=avg_pm25_corr,
        used_color_hex=color,
        strategy="average_fresh_sensors",
        comparison=comparison_row,
    )
    payload["light"]["color_hex_category"] = color
    payload["light"]["lifx_command_sent"] = lifx_command_color
    payload["light"]["confidence"] = comparison_row["confidence"]
    write_status_json(payload)


if __name__ == "__main__":
    if manual_override():
        exit(0)

    try:
        main()
    except Exception as e:
        print(f"FATAL ERROR: {e}")

        # Fallback to white
        try:
            set_lifx_color("white")
            print("Set LIFX to WHITE as fallback.")
        except Exception as e2:
            print(f"Failed to set fallback white color: {e2}")

        exit(1)
