# Weather Data Source Update Frequency & Latency

Empirical measurements from actual API/S3 pulls. Run `python -m research.download_data.measure_data_source_latency` to refresh.

## Summary Table

| Data Source | Update Frequency | Latency (Access vs Generated) | Notes |
|-------------|------------------|-------------------------------|-------|
| **HRRR** | Hourly (00Z–23Z) | ~1.5 hr | NOAA HRRR via AWS S3; f00 available ~45–90 min after cycle |
| **METAR** | Hourly + SPECI | ~1.5 hr | AWC API; routine obs at :55 past hour; specials when conditions change |
| **NBM** | Hourly (00Z–23Z) | ~1.5 hr | NOAA NBM via AWS S3; blend of GFS, HRRR, NAM, etc. |
| **RTMA** | Every 15 min | ~29 min | RTMA-RU 15-min analysis; fastest refresh of the four |

## Measurement Details

- **Latency** = time from when data is valid/generated until we successfully fetch it.
- **Measured**: 2026-02-22 (single run).
- **Station**: KMDW (Chicago Midway).
- **Sources**: HRRR sfc, AWC METAR API, RTMA-RU anl, NBM co.

### Latest Empirical Pull

| Source | Valid Time (UTC) | Latency |
|--------|------------------|---------|
| METAR | 2026-02-22 22:00Z | 1.5 hr |
| HRRR | 2026-02-22 22:00Z | 1.5 hr |
| RTMA | 2026-02-22 23:00Z | 28.7 min |
| NBM | 2026-02-22 22:00Z | 1.5 hr |

## Update Frequency (Documented)

| Source | Schedule | Reference |
|--------|----------|-----------|
| **HRRR** | Hourly cycles | [NOAA HRRR](https://rapidrefresh.noaa.gov/hrrr/) |
| **METAR** | Hourly routine + irregular SPECI | [AWC](https://aviationweather.gov/) |
| **NBM** | Hourly (short-range); 3h/6h for extended | [NCEP NBM](https://www.nco.ncep.noaa.gov/pmb/products/blend/) |
| **RTMA-RU** | Every 15 min (00, 15, 30, 45 past hour) | [NCEP RTMA](https://www.nco.ncep.noaa.gov/pmb/products/rtma/) |

## How to Re-run Measurements

```bash
python -m research.download_data.measure_data_source_latency
```

Results are written to `data/weather_source_latency.json`.
