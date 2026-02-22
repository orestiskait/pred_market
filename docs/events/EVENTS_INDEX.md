# Event Resolution Documentation

Documentation for Kalshi weather event resolution, data sources, and technical details that affect contract settlement.

| Document | Description |
|----------|-------------|
| [kalshi_settlement_rules.md](kalshi_settlement_rules.md) | **Kalshi-specific:** Standard Time rule, Chicago Midway vs O'Hare (CLIMDW/CLIORD), settlement timing |
| [weather_prediction_rules.md](weather_prediction_rules.md) | NWS station types, designated stations, rounding rules, and official high calculation |
| [asos_temperature_resolution.md](asos_temperature_resolution.md) | ASOS technical specs: sampling frequency, accuracy, rounding, °F vs °C, lags, and implications for Kalshi |

---

## Cross-check: discrepancies between documents

The two docs above were written from different sources. Where they disagree, the ASOS technical analysis is treated as the more authoritative reference (it aligns with Federal Meteorological Handbooks and NWS/ASOS documentation).

| Topic | Weather prediction rules | ASOS temperature resolution |
|-------|--------------------------|-----------------------------|
| **Official high calculation** | Highest **1-minute average** recorded | **2-minute average** (or 5-minute running average) of samples — *not* instantaneous 1-min peak |
| **Official high vs 5-min time series** | Official high **can be higher** than any 5-min value | Official high **can be lower** than 1-min or 5-min values (cooling bias ~0.5–1.0°C) |
| **Internal units** | Hourly: 0.1°F → 0.1°C → F | ASOS operates internally in **Celsius** (0.1°C resolution) |

**Recommendation:** Use `asos_temperature_resolution.md` for how the official high is actually computed (2/5-min averaging), and treat the project rules as an older summary that may need updating for consistency.
