"""
rebuild_historical_comparison.py

One-off rebuild of the PurpleAir-vs-official-AQHI comparison log, going
back as far as both data sources actually allow (requested 2026-09-01):
- PurpleAir hourly readings (Supabase sensor_readings): 3 of the 4
  sensors go back to 2025-01-01; the 4th (249949) starts 2026-01-27.
- Official AQHI (government OData API): hard 365-day rolling retention,
  confirmed empirically - nothing before ~2025-09-01 is queryable at all.

So 2025-09-01 is the real start of usable overlap, not 2025-01-01 - the
government side is the limiting factor. Requested cadence is hourly (the
live script runs every ~25-30min; this only needs one row per hour).

Reproduces update_light_pa.py's comparison logic exactly:
- estimated_aqhi = floor(pm25_corrected_avg / 10) + 1
- official = inverse-distance-weighted avg of the 3 closest stations to
  the fixed sensor-centroid site location, rounded with math.ceil
- confidence = high (diff<=1) / medium (diff<=2) / low (diff>2)
using the SAME sensor set changes over time as the live script (3
sensors until 249949 comes online, then 4).
"""
import math
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

SENSOR_IDS = [83971, 91545, 166965, 249949]
STATIONS = ["Drayton Valley", "Genesee", "Enoch"]
MEAS_URL = "https://data.environment.alberta.ca/EdwServices/aqhi/odata/StationMeasurements"

START = datetime(2025, 9, 1, tzinfo=timezone.utc)
END = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

OUT_PATH = "data/aqhi_comparison_log_rebuilt.csv"

CONFIDENCE_HIGH_MAX_DIFF = 1
CONFIDENCE_MEDIUM_MAX_DIFF = 2


def fetch_sensor_readings(sensor_id):
    """Page through Supabase sensor_readings for one sensor, full range."""
    rows = []
    page_size = 1000
    offset = 0
    headers = {
        "apikey": SUPABASE_SERVICE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}",
    }
    while True:
        params = {
            "sensor_index": f"eq.{sensor_id}",
            "recorded_at": [f"gte.{START.isoformat()}", f"lte.{END.isoformat()}"],
            "select": "recorded_at,pm_corrected,humidity",
            "order": "recorded_at.asc",
            "limit": str(page_size),
            "offset": str(offset),
        }
        r = requests.get(f"{SUPABASE_URL}/rest/v1/sensor_readings", headers=headers, params=params, timeout=30)
        r.raise_for_status()
        batch = r.json()
        rows.extend(batch)
        if len(batch) < page_size:
            break
        offset += page_size
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["recorded_at"] = pd.to_datetime(df["recorded_at"], utc=True)
    df["sensor_index"] = sensor_id
    return df


def fetch_station_aqhi(station_name):
    """Page through the government API for one station's AQHI (blank
    ParameterName) history, full range."""
    rows = []
    # The server hard-caps $top at 5000 with NO @odata.nextLink at all -
    # confirmed empirically (a full-year query silently truncated to the
    # first 5000 hours, ~mid-March 2026, with no signal it had done so).
    # $skip does work though, so page manually instead of trusting nextLink.
    filter_clause = (
        f"StationName eq '{station_name}' and ParameterName eq null "
        f"and ReadingDate ge {START.strftime('%Y-%m-%dT%H:%M:%SZ')} "
        f"and ReadingDate le {END.strftime('%Y-%m-%dT%H:%M:%SZ')}"
    )
    page_size = 5000
    skip = 0
    while True:
        params = {
            "$filter": filter_clause,
            "$select": "StationName,Value,ReadingDate",
            "$orderby": "ReadingDate asc",
            "$top": str(page_size),
            "$skip": str(skip),
        }
        r = requests.get(MEAS_URL, params=params, timeout=30)
        r.raise_for_status()
        batch = r.json().get("value", [])
        rows.extend(batch)
        if len(batch) < page_size:
            break
        skip += page_size
    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["ReadingDate"] = pd.to_datetime(df["ReadingDate"], utc=True).dt.floor("h")
    return df


def rh_correct_pm25(pm25_raw, rh):
    if rh is None or pd.isna(rh):
        rh = 50.0
    rh = float(rh)
    if rh < 30.0:
        denom = 1.0 + 0.24 / (100.0 / 30.0 - 1.0)
    elif rh < 70.0:
        denom = 1.0 + 0.24 / (100.0 / rh - 1.0)
    else:
        denom = 1.0 + 0.24 / (100.0 / 70.0 - 1.0)
    return float(pm25_raw) / denom


def haversine_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dlon / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def main():
    print(f"Range: {START.isoformat()} to {END.isoformat()}")

    # ---- PurpleAir sensors ----
    sensor_frames = []
    for sid in SENSOR_IDS:
        print(f"[pull] sensor_readings for {sid}...")
        df = fetch_sensor_readings(sid)
        print(f"  -> {len(df)} rows")
        if not df.empty:
            sensor_frames.append(df)
        time.sleep(0.2)
    sensors = pd.concat(sensor_frames, ignore_index=True)
    # pm_corrected is already RH-corrected in the Supabase pipeline (see
    # AB_PA_latest.py's correct_pm25) - same formula as update_light_pa.py's
    # rh_correct_pm25, so no need to recompute here.

    # station metadata (lat/lon) for the site centroid - use the sensor's
    # OWN known coordinates via the existing meta table rather than
    # recomputing per-row (fixed hardware, doesn't move).
    headers = {"apikey": SUPABASE_SERVICE_KEY, "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}"}
    meta_r = requests.get(
        f"{SUPABASE_URL}/rest/v1/purpleair_sensors_meta",
        headers=headers,
        params={"sensor_index": f"in.({','.join(str(s) for s in SENSOR_IDS)})",
                "select": "sensor_index,latitude,longitude"},
        timeout=30,
    )
    meta_r.raise_for_status()
    meta = {row["sensor_index"]: (row["latitude"], row["longitude"]) for row in meta_r.json()}
    print(f"[pull] sensor metadata: {meta}")

    # ---- Official AQHI stations ----
    station_frames = []
    for st in STATIONS:
        print(f"[pull] official AQHI for {st}...")
        df = fetch_station_aqhi(st)
        print(f"  -> {len(df)} rows")
        if not df.empty:
            df["StationName"] = st
            station_frames.append(df)
        time.sleep(0.2)
    official = pd.concat(station_frames, ignore_index=True)

    # Station lat/lon aren't in the official-AQHI pull above (only in the
    # AB_datapull stations feed) - one small fetch to get the 3 fixed
    # coordinates.
    station_coords = {}
    stations_csv = pd.read_csv(
        "https://raw.githubusercontent.com/DKevinM/AB_datapull/main/data/last6h.csv"
    )
    coord_rows = stations_csv[stations_csv["StationName"].isin(STATIONS)][
        ["StationName", "Latitude", "Longitude"]
    ].drop_duplicates(subset="StationName")
    for _, r in coord_rows.iterrows():
        station_coords[r["StationName"]] = (r["Latitude"], r["Longitude"])
    print(f"[pull] station coords: {station_coords}")

    # ---- Build hourly comparison rows ----
    hours = pd.date_range(START, END, freq="h", tz=timezone.utc)
    out_rows = []

    for hour in hours:
        # which sensors have data this hour (249949 only from 2026-01-27 on)
        hour_sensors = sensors[sensors["recorded_at"] == hour]
        if hour_sensors.empty:
            continue

        usable = []
        for _, srow in hour_sensors.iterrows():
            sid = srow["sensor_index"]
            pm_corr = srow["pm_corrected"]
            if pd.isna(pm_corr):
                continue
            lat, lon = meta.get(sid, (None, None))
            usable.append({"sensor_index": sid, "pm25_corr": pm_corr, "latitude": lat, "longitude": lon})

        if not usable:
            continue

        used_sensor_indices = ";".join(str(s["sensor_index"]) for s in usable)
        avg_pm25_corr = sum(s["pm25_corr"] for s in usable) / len(usable)
        estimated_aqhi = math.floor(avg_pm25_corr / 10) + 1

        lats = [s["latitude"] for s in usable if s["latitude"] is not None]
        lons = [s["longitude"] for s in usable if s["longitude"] is not None]
        site_lat = sum(lats) / len(lats) if lats else None
        site_lon = sum(lons) / len(lons) if lons else None

        hour_official = official[official["ReadingDate"] == hour]
        station_vals = {}
        for _, orow in hour_official.iterrows():
            station_vals[orow["StationName"]] = orow["Value"]

        with_dist = []
        for st in STATIONS:
            if st not in station_vals or pd.isna(station_vals[st]):
                continue
            if st not in station_coords or site_lat is None:
                continue
            slat, slon = station_coords[st]
            dist = haversine_km(site_lat, site_lon, slat, slon)
            with_dist.append({"station": st, "distance": dist, "value": station_vals[st]})
        with_dist.sort(key=lambda s: s["distance"])
        closest3 = with_dist[:3]

        if closest3:
            weighted_sum = sum(s["value"] / max(s["distance"], 0.1) for s in closest3)
            weight_total = sum(1.0 / max(s["distance"], 0.1) for s in closest3)
            official_avg = weighted_sum / weight_total
            official_rounded = math.ceil(official_avg)
        else:
            official_avg = None
            official_rounded = None

        if estimated_aqhi is not None and official_rounded is not None:
            diff = estimated_aqhi - official_rounded
            adiff = abs(diff)
            confidence = "high" if adiff <= CONFIDENCE_HIGH_MAX_DIFF else (
                "medium" if adiff <= CONFIDENCE_MEDIUM_MAX_DIFF else "low"
            )
        else:
            diff = None
            confidence = "unknown"

        row = {
            "timestamp_utc": hour.isoformat(),
            "n_sensors_used": len(usable),
            "used_sensor_indices": used_sensor_indices,
            "pm25_corr_avg": round(avg_pm25_corr, 2),
            "estimated_aqhi": estimated_aqhi,
            "site_lat": site_lat,
            "site_lon": site_lon,
        }
        for i, s in enumerate(closest3, start=1):
            row[f"station{i}_name"] = s["station"]
            row[f"station{i}_km"] = round(s["distance"], 2)
            row[f"station{i}_aqhi"] = s["value"]
        for i in range(len(closest3) + 1, 4):
            row[f"station{i}_name"] = None
            row[f"station{i}_km"] = None
            row[f"station{i}_aqhi"] = None
        row["official_weighted_avg"] = round(official_avg, 2) if official_avg is not None else None
        row["official_rounded"] = official_rounded
        row["diff_estimated_minus_official"] = diff
        row["confidence"] = confidence

        out_rows.append(row)

    out_df = pd.DataFrame(out_rows)
    out_df.to_csv(OUT_PATH, index=False)
    print(f"\nWrote {len(out_df)} rows to {OUT_PATH}")


if __name__ == "__main__":
    main()
