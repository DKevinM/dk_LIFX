# Co-location data (seasonal correction, paper Section 5.4)

These files support the monthly seasonal correction derived from PurpleAir sensors co-located with official AQHI stations elsewhere in Alberta.

| File | Contents |
|---|---|
| `station_nearest_sensor.csv` | Each of Alberta's 87 official AQHI stations, its nearest community PurpleAir sensor, and the distance in km. The nine pairs within 0.6 km form the co-location set. |
| `pooled_colocated_diffs.csv` | One row per valid hour: `diff` = PurpleAir PM2.5-only estimate minus official AQHI, with `month`, `year` and `site`. Covers 2025 and 2026 (65,194 and 30,447 rows). Woodcroft is not included; its reference is an older TEOM monitor. |
| `colocated_monthly.csv` | Descriptive table: the 2025 monthly mean difference for each site. Its `MEAN (excl Woodcroft)` column is the average of the site means. **It is not the correction.** |

## Reproducing the correction

The correction (`MONTHLY_AQHI_CORRECTION` in `update_light_pa.py`) is the negated, **hour-weighted** mean of `pooled_colocated_diffs.csv`, restricted to **2025** and grouped by calendar month:

```python
import pandas as pd
p = pd.read_csv("data/pooled_colocated_diffs.csv")
p = p[(p.year == 2025) & (p.site != "Woodcroft")]
correction = (-p.groupby("month")["diff"].mean()).round(2)
```

This reproduces all 12 factors exactly (1.07, 1.19, 1.08, 1.34, 1.26, 0.69, 0.58, 0.54, 0.27, 0.65, 0.57, 0.65), with 4,229–5,952 pooled hours per month. The 2026 rows are the held-out period used to test the correction and are not part of its derivation.

Averaging the per-site monthly means instead (the `MEAN` column in `colocated_monthly.csv`) matches only 7 of the 12 factors, because sites with more valid hours carry proportionally more weight in the pooled mean.
