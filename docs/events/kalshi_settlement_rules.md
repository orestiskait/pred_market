# Kalshi Settlement Rules — NWS Alignment

Kalshi follows the National Weather Service (NWS) strictly for weather market resolution. This document captures Kalshi-specific rules that affect contract settlement and data interpretation.

---

## 1. The "Standard Time" Rule

The NWS records climate data in **Local Standard Time (LST)** year-round, even during Daylight Saving Time (DST). This introduces a specific quirk:

| Period | What it means |
|--------|----------------|
| **Climate day boundary** | Midnight-to-midnight in **Standard Time**, not local clock time |
| **During DST** | The "Tuesday" daily high covers **1:00 AM Tuesday to 12:59 AM Wednesday** in local clock time |

**Example (Chicago, CDT):** The "Feb 22" market resolves to the high recorded for the NWS climate day Feb 22. In CST, that is midnight Feb 22 → midnight Feb 23. In local Chicago clock (CDT), that is **1:00 AM Feb 22 → 12:59 AM Feb 23**.

**Implication:** When comparing real-time observations (ASOS, METAR) to the official daily high, use the station's LST offset, not its current UTC offset. The project's `lst_offset_hours()` in `iem_awc_station_registry` uses a winter date to compute the Standard Time offset.

---

## 2. Chicago: Midway vs O'Hare

Kalshi typically defaults to **Midway** for "Chicago" markets unless specified otherwise.

| Station | ICAO | NWS CLI Product | Use |
|---------|------|-----------------|-----|
| **Chicago Midway** | KMDW | CLIMDW | **Default for Kalshi Chicago markets** |
| Chicago O'Hare | KORD | CLIORD | Alternative; use only if market specifies O'Hare |

The project registry maps `KXHIGHCHI` → **KMDW** (Midway). Do not switch to KORD unless a market explicitly uses O'Hare.

---

## 3. Settlement Timing

Markets usually settle the **following morning** once the NWS releases the finalized **Daily Climatological Report** (CLI).

| Step | Timing |
|------|--------|
| Climate day ends | Midnight LST (e.g. 12:59 AM local clock during DST) |
| DSM issued | ASOS station sends Daily Summary Message ~01:00 local time |
| CLI published | NWS Daily Climate Report typically available ~06:00 UTC |
| Kalshi settlement | Usually same morning after CLI is released |

The IEM CLI fetcher (`iem_daily_climate.py`) retrieves this data. Expect a 6–12 hour lag after the climate day ends before the official high is available.

---

## 4. Cross-References

- **Day boundary in code:** `research.weather.iem_awc_station_registry.lst_offset_hours()` — uses Standard Time
- **Event rollover:** `docs/EVENT_ROLLOVER.md` — market selection uses NWS-aligned timezone
- **Station mapping:** `services/markets/registry.py` — KXHIGHCHI → KMDW
- **Official high source:** `docs/events/asos_temperature_resolution.md` — DSM/CLI technical details
