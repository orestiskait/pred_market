# ASOS Temperature Resolution — Kalshi-Relevant Technical Summary

This document summarizes the technical characteristics of ASOS (Automated Surface Observing System) temperature data that affect **Kalshi contract resolution** and near-real-time proxies. It focuses on sampling frequency, accuracy, rounding, Fahrenheit vs Celsius, and data-source lags.

---

## 1. What Kalshi Uses: Official Daily High

Kalshi resolves temperature-high contracts using the **NWS Daily Climate Report (CLI)**. The official daily high comes from the **Daily Summary Message (DSM)**, issued by the ASOS station around 01:00 local time for the previous day.

| Attribute | Value |
|-----------|-------|
| **Product** | DSM → CLI (Daily Climate Report) |
| **Calculation** | 2-minute average of 2–5 second samples (or 5-minute running average of 1-min samples) |
| **Units** | Whole degrees **Fahrenheit** |
| **Lag** | ~6–12 hours after midnight (post-occurrence) |

**Important:** The official high is **not** an instantaneous 1-minute peak. It is a **2-minute (or 5-minute) average** designed to match historical liquid-in-glass thermometers. This introduces a **cooling bias** of ~0.5–1.0°C compared to true instantaneous peaks.

---

## 2. Sampling Frequency and Temporal Resolution

| Data Product | Sampling / Averaging | Temporal Resolution | Lag |
|--------------|----------------------|----------------------|-----|
| **ASOS 1-min (IEM/NCEI)** | 1-minute average of 2–10 s samples | Every 60 seconds | 18–36 hours |
| **Official High (DSM/CLI)** | 2-min avg (or 5-min running avg) | Daily summary | 6–12 hours |
| **METAR (body)** | Instantaneous / 1-min avg at obs time | Hourly | Minutes |
| **METAR (6hr max/min)** | Same peak logic as official | At 00Z, 06Z, 12Z, 18Z | Minutes |
| **5-minute report** | 1-min sample at that time | Every 5 minutes | Minutes |

- **Internal ASOS sampling:** 2–10 seconds (DCP/ACU).
- **1-minute data:** Each value is a **1-minute average**, not an instantaneous reading. Often derived from two 30-second samples.
- **Official high:** Uses 2-minute or 5-minute averaging, so it can be **lower** than any single 1-minute or 5-minute value you see in real-time.

---

## 3. Accuracy and Precision

| Specification | Value |
|---------------|-------|
| **Sensor accuracy** | ±0.6°C (−50°C to +50°C) |
| **Internal resolution** | 0.1°C |
| **Official high (CLI)** | Whole degree Fahrenheit |
| **METAR body** | Integer Celsius |
| **METAR T-group (RMK)** | 0.1°C (e.g. `T01830102` = 18.3°C, 10.2°C dew point) |
| **1-minute archive** | 0.1°C or 0.1°F |

---

## 4. Rounding and Fahrenheit vs Celsius

### Round-tripping and conversion errors

- ASOS operates internally in **Celsius**.
- The official high is reported in **whole degrees Fahrenheit**.
- Some products (e.g. 5-minute METAR) round to **integer Celsius** before conversion back to Fahrenheit for display.

This creates:

1. **Round-tripping error:** F → C → F conversions can shift values by 1–2°F.
2. **Ambiguity:** One Celsius value can map to two different Fahrenheit temperatures; if those span different strike levels, the data is ambiguous for betting.
3. **5-minute vs official:** Public 5-minute displays often show “instantaneous” 1-min samples, while the official high uses 2/5-minute averages. A 5-minute web value can be **higher** than the final official high.

### Example

- 1-min readings: 84, 84, 84, 85, 84°F  
- 5-minute average: (84+84+84+85+84)/5 = **84.2°F** → rounded to **84°F**  
- A single 85°F reading may appear on a 5-minute site, but the official high for that period could be 84°F.

---

## 5. Data Source Comparison for Kalshi

| Source | Temp Reported | Rounding | Lag | Use for Kalshi |
|--------|---------------|----------|-----|----------------|
| **CLI (Official)** | Daily high/low | Whole °F | 6–12 h | **Settlement** |
| **ASOS 1-min (IEM)** | 1-min average | 0.1°C/°F | 18–36 h | Research, backtesting |
| **METAR body** | At obs time | Integer °C | Minutes | Near-real-time proxy |
| **METAR T-group** | At obs time | 0.1°C | Minutes | Higher precision |
| **METAR 6hr max/min** | 6-hour extremes | Integer °C | Minutes | Partial-day proxy |

---

## 6. Implications for Kalshi Betting

1. **Official high is conservative:** Expect it to be ~0.5–1.0°C **lower** than true instantaneous peaks.
2. **5-minute / METAR can overstate:** Real-time 5-minute or hourly values can exceed the official high due to averaging.
3. **Rounding matters at strike boundaries:** At 75°F vs 76°F, conversion and rounding can change outcomes.
4. **1-minute data is delayed:** Not usable for same-day resolution; use for analysis and backtesting only.
5. **METAR 6hr max/min:** Available at synoptic hours (00Z, 06Z, 12Z, 18Z); useful as a partial-day proxy but not identical to the daily official high.

---

## 7. Quick Reference: Product Summary

| Product | Frequency | Temp Precision | Averaging | Primary Use |
|---------|-----------|----------------|-----------|-------------|
| METAR (body) | Hourly | Integer °C | Instant/1-min | Aviation |
| METAR (T-group) | Hourly | 0.1°C | At obs time | Research |
| DSM/CLI | Daily | Whole °F | 2-min avg | **Kalshi settlement** |
| 1-min (DSI-3285) | 1 min | 0.1°C/°F | 1-min avg | High-res research |
| 5-min report | 5 min | Integer °C | 1-min sample | General aviation |

---

*Source: Technical analysis of ASOS temperature reporting and climatological validation (NWS, FAA, DOD tri-agency system).*
