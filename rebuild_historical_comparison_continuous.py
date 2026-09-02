"""
rebuild_historical_comparison_continuous.py

Rebuild of the PurpleAir-vs-official-AQHI comparison log using a
CONTINUOUS (non-rounded) official AQHI reference, computed directly from
raw station pollutant concentrations (Ozone, Nitrogen Dioxide, Fine
Particulate Matter) via the Stieb et al. (2008) formula, rather than
relying on AEPA's own pre-rounded integer AQHI field.

Why this exists: the original rebuild_historical_comparison.py IDW-
averages the 3 reference stations' already-ROUNDED integer AQHI values
and then applies math.ceil() to the average - two compounding rounding
steps. A spot-check against ECCC's independent continuous AQHI product
(same physical stations, GeoMet API) showed this shifts the final
official AQHI category in ~35% of hours, always upward. This script
avoids that by rounding only once, at the very end, after blending
continuous per-station values.

CRITICAL: earlier attempts to reproduce ECCC's continuous AQHI from raw
pollutants were off by ~0.76 units due to a coefficient transcription
bug (O3 and NO2 coefficients swapped). Corrected coefficients per
Stieb et al. (2008), confirmed against 4 independent citing papers and
validated to within 0.005-0.01 AQHI units of ECCC's own continuous
values for Drayton Valley and Genesee:
    AQHI = (10/10.4) * 100 * [(exp(0.000537*O3_ppb) - 1)
                             + (exp(0.000871*NO2_ppb) - 1)
                             + (exp(0.000487*PM25) - 1)]
using 3-hour trailing rolling averages of each pollutant.

Data availability: originally pulled from the government's live OData
API, which has a hard 365-day rolling retention (confirmed empirically)
- capping history at ~2025-09-02. Switched to Supabase's own `aqhi_data`
table instead: fetch_push.py has always pushed EVERY ParameterName (not
just the blank/AQHI rows), and since nothing has ever been purged from
that table, it already holds Ozone/NO2/Fine Particulate Matter for all
3 stations back to 2025-01-01, the full start of the PurpleAir record.
NOTE: fetch_push.py's clean_data() converts gas parameters from ppm to
ppb BEFORE pushing (see PPM_PARAMS/ppm_mask there), so values pulled
from aqhi_data are already in ppb - do NOT re-apply a *1000 conversion
here (unlike a raw pull straight from the government API, which is
still in ppm).
"""
import math
import os
import time
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests

from rebuild_historical_comparison import fetch_sensor_readings, haversine_km

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_KEY = os.environ["SUPABASE_SERVICE_KEY"]

SENSOR_IDS = [83971, 91545, 166965, 249949]
STATIONS = ["Drayton Valley", "Genesee", "Enoch"]
POLLUTANTS = ["Ozone", "Nitrogen Dioxide", "Fine Particulate Matter"]

START = datetime(2025, 1, 1, tzinfo=timezone.utc)
END = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

OUT_PATH = "data/aqhi_comparison_log_continuous.csv"

CONFIDENCE_HIGH_MAX_DIFF = 1
CONFIDENCE_MEDIUM_MAX_DIFF = 2

# Stieb et al. (2008) coefficients - verified against 4 independently
# citing papers (Szyszkowicz & Kousha 2014; Szyszkowicz & de Angelis
# 2022; Brankston et al. 2020; Logothetis et al. 2023), all agreeing:
BETA_O3 = 0.000537
BETA_NO2 = 0.000871
BETA_PM25 = 0.000487


def fetch_station_pollutants(station_name):
    """Page through Supabase's aqhi_data table for one station's raw
    Ozone/NO2/Fine Particulate Matter, full 2025-01-01-to-now range.
    Values are already in ppb for gases (fetch_push.py converts before
    pushing) - see module docstring."""
    rows = []
    headers = {"apikey": SUPABASE_SERVICE_KEY, "Authorization": f"Bearer {SUPABASE_SERVICE_KEY}"}
    page_size = 1000  # PostgREST's real cap, confirmed empirically
    for param in POLLUTANTS:
        offset = 0
        while True:
            params = {
                "StationName": f"eq.{station_name}",
                "ParameterName": f"eq.{param}",
                "ReadingDate": [f"gte.{START.isoformat()}", f"lte.{END.isoformat()}"],
                "select": "ParameterName,Value,ReadingDate",
                "order": "ReadingDate.asc",
                "limit": str(page_size),
                "offset": str(offset),
            }
            r = requests.get(f"{SUPABASE_URL}/rest/v1/aqhi_data", headers=headers, params=params, timeout=30)
            r.raise_for_status()
            batch = r.json()
            rows.extend(batch)
            if len(batch) < page_size:
                break
            offset += page_size
    df = pd.DataFrame(rows)
    df["ReadingDate"] = pd.to_datetime(df["ReadingDate"], utc=True).dt.floor("h")
    return df


def compute_continuous_aqhi(df):
    """3-hr trailing rolling average per pollutant -> Stieb formula.
    Returns a Series indexed by hour (UTC)."""
    piv = df.pivot_table(index="ReadingDate", columns="ParameterName", values="Value", aggfunc="mean").sort_index()
    piv = piv.asfreq("h")  # fill any gaps so rolling(3) doesn't silently bridge missing hours
    # already in ppb - fetch_push.py converts gas parameters ppm->ppb before pushing to aqhi_data
    o3_ppb = piv["Ozone"]
    no2_ppb = piv["Nitrogen Dioxide"]
    pm25 = piv["Fine Particulate Matter"]

    o3_3hr = o3_ppb.rolling(3, min_periods=3).mean()
    no2_3hr = no2_ppb.rolling(3, min_periods=3).mean()
    pm25_3hr = pm25.rolling(3, min_periods=3).mean()

    aqhi = (10.0 / 10.4) * 100.0 * (
        (pd.Series.apply(BETA_O3 * o3_3hr, lambda x: math.exp(x) - 1))
        + (pd.Series.apply(BETA_NO2 * no2_3hr, lambda x: math.exp(x) - 1))
        + (pd.Series.apply(BETA_PM25 * pm25_3hr, lambda x: math.exp(x) - 1))
    )
    return aqhi.dropna()


def main():
    print(f"Range: {START.isoformat()} to {END.isoformat()}")

    sensor_frames = []
    for sid in SENSOR_IDS:
        print(f"[pull] sensor_readings for {sid}...")
        df = fetch_sensor_readings(sid)
        print(f"  -> {len(df)} rows")
        if not df.empty:
            sensor_frames.append(df)
        time.sleep(0.2)
    sensors = pd.concat(sensor_frames, ignore_index=True)
    sensors = sensors[sensors["recorded_at"] >= START]

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

    print("[pull] official station coordinates...")
    stations_csv = pd.read_csv("https://raw.githubusercontent.com/DKevinM/AB_datapull/main/data/last6h.csv")
    coord_rows = stations_csv[stations_csv["StationName"].isin(STATIONS)][
        ["StationName", "Latitude", "Longitude"]
    ].drop_duplicates(subset="StationName")
    station_coords = {r["StationName"]: (r["Latitude"], r["Longitude"]) for _, r in coord_rows.iterrows()}
    print(f"  -> {station_coords}")

    continuous_aqhi = {}
    for st in STATIONS:
        print(f"[pull] raw pollutants for {st}...")
        df = fetch_station_pollutants(st)
        print(f"  -> {len(df)} raw rows")
        continuous_aqhi[st] = compute_continuous_aqhi(df)
        print(f"  -> {len(continuous_aqhi[st])} hourly continuous AQHI values")
        time.sleep(0.2)

    hours = pd.date_range(START, END, freq="h", tz=timezone.utc)
    out_rows = []

    for hour in hours:
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

        with_dist = []
        for st in STATIONS:
            series = continuous_aqhi.get(st)
            if series is None or hour not in series.index:
                continue
            val = series.loc[hour]
            if pd.isna(val) or st not in station_coords or site_lat is None:
                continue
            slat, slon = station_coords[st]
            dist = haversine_km(site_lat, site_lon, slat, slon)
            with_dist.append({"station": st, "distance": dist, "value": val})
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
            row[f"station{i}_aqhi_continuous"] = round(s["value"], 3)
        for i in range(len(closest3) + 1, 4):
            row[f"station{i}_name"] = None
            row[f"station{i}_km"] = None
            row[f"station{i}_aqhi_continuous"] = None
        row["official_weighted_avg"] = round(official_avg, 3) if official_avg is not None else None
        row["official_rounded"] = official_rounded
        row["diff_estimated_minus_official"] = diff
        row["confidence"] = confidence

        out_rows.append(row)

    out_df = pd.DataFrame(out_rows)
    out_df.to_csv(OUT_PATH, index=False)
    print(f"\nWrote {len(out_df)} rows to {OUT_PATH}")


if __name__ == "__main__":
    main()
