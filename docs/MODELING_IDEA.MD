# Kalshi Daily High Temperature Quantitative Trading Model (Updated)

## System Architecture Overview
The system is divided into two decoupled engines:
1.  **The Weather Brain (ML Model):** Predicts exactly how much hotter the physical atmosphere will get before the climate day ends, while accounting for the uncertainty of 5-minute rounded data.
2.  **The Execution Brain (Trading Logic):** Maps the Weather Brain's physical forecast against every active Kalshi strike contract to find mispriced expected value (EV) and execute trades.

---

## 1. The Response Variable (Target Label)
Instead of predicting the absolute peak temperature, the model predicts the **Remaining Heating Delta**. This solves the issue of non-stationarity and natively handles post-peak data (when the remaining delta converges to zero).

* **Definition**: $Y = Final\_CLI\_High - Custom\_Intraday\_Max_t$
* **Historical Target Construction**: Extract the $Final\_CLI\_High$ from the historical NWS Daily Climate Report batch data. The $Custom\_Intraday\_Max_t$ is the historical 5-minute rounded high-water mark calculated up to the specific point in time $t$.
* **Model Type**: **Quantile Regression** (e.g., LightGBM with Pinball Loss). Instead of predicting a single mean value, the model outputs an array of percentiles (e.g., 10th, 50th, 90th percentile) for the delta, effectively generating a continuous Probability Density Function (PDF).

---

## 2. The Weather Brain: Features & Time Alignment
Time alignment is the most critical guardrail in backtesting. The features must strictly respect processing and publishing latencies to avoid look-ahead bias. Kalshi strikes are strictly excluded from these inputs.

### A. The "State of the Day" Envelope (5-Minute Frequency)
*Alignment: Instantaneous. Pulled from live Wethr/Synoptic 5-minute ASOS streams.* (weather push observations product == hf metar)

* **`custom_intraday_max`**: The bedrock of your system. This is the maximum of:
    1.  The highest 5-minute rounded peak observed so far today.
    2.  The highest precise 0.1°C temperature parsed from any METAR T-group remarks received so far today (converted to Fahrenheit).
    *Rule: You must hard-reset this calculation at exactly 00:00 Local Standard Time (LST), ignoring Daylight Saving Time completely.*
* **`peak_high_5min`**: The highest 1-minute high-water mark recorded within the current 5-minute window. While still rounded, it provides the "high point" of the 5-minute interval.
* **`t_group_divergence`**: `custom_intraday_max - latest_hourly_t_group_max`. Tracks the hidden rounding error; if the 5-minute peak is 75°F but the T-group is 74.1°F, the model learns the peak is sitting on the bottom edge of the rounding threshold.

### B. Micro-Momentum (5-Minute Frequency)
*Alignment: Instantaneous.*

* **`velocity_15m`**: Current 5-minute rounded peak minus the 5-minute rounded peak from 15 minutes ago.
* **`acceleration_15m`**: The current `velocity_15m` minus the `velocity_15m` from 5 minutes ago. (Negative values indicate flattening heating curves).
* **`oscillation_count`**: Number of times the 5-minute peak has fluctuated between whole integers over the last 30 minutes, signaling proximity to a rounding boundary.

### C. NWP Baseline, Quantiles & Calibration (Hourly / 15-Minute)
*Alignment: Strictly lagged to reflect processing delays (~1.5 hours for HRRR/NBM, ~29 mins for RTMA-RU).*

* **`hrrr_expected_remaining_delta`**: The highest 15-minute temperature forecasted in the remainder of the climate day from the HRRR `subh` product, minus the `custom_intraday_max`.
* **`hrrr_current_error_f`**: The high-precision METAR T-group temperature minus the HRRR forecast valid for that exact observation hour.
* **`rtma_current_divergence`**: Current 5-minute rounded temperature minus the latest available RTMA-RU analysis.
* **`neighbor_lake_gradient`**: The temperature difference between your target station (e.g., KMDW) and a coastal/adjacent station (e.g., KORD) using available 5-minute data. Used to detect approaching localized fronts like lake breezes.
* **NBM Probabilistic Quantiles (The Shrinking Horizon)**: Because the NBM is a statistically post-processed blend, it provides probabilistic grids.
    * **`nbm_forecast_p90`**: The **maximum** of all remaining hourly 90th-percentile NBM forecasts until midnight LST.
    * **`nbm_forecast_p10`**: The **maximum** of all remaining hourly 10th-percentile NBM forecasts until midnight LST.
    * **`nbm_spread`**: `nbm_forecast_p90 - nbm_forecast_p10`. A narrow spread signals high model consensus.

> **Note on RRFS (Rapid Refresh Forecast System)**: RRFS is NOAA's next-generation model designed to replace HRRR. As of early 2026, data generation is paused in a pre-operational state. Exclude RRFS from live ML inference for now.

---

## 3. The Execution Brain: Mapping to Strikes & Probabilities

Once your Weather Brain predicts the Probability Density Function (PDF) of the remaining heating delta ($\Delta_{remaining}$), the Execution Brain loops through every active Kalshi contract.

### Step 1: Define the Required Delta per Kalshi Contract
Kalshi generates a daily series with discrete strikes. For *each* active contract, calculate the physical heat required to trigger a YES payout:
$Required\_Delta = Strike\_Temperature - custom\_intraday\_max$

### Step 2: Extract the Cumulative Probability
Query your ML model's Quantile Regression output to find the probability that the actual heating will be greater than or equal to the $Required\_Delta$.
* *Example*: `custom_intraday_max` is 72°F. You evaluate the "High above 74°F" contract.
* The $Required\_Delta$ is 2.0°F. You query the model's CDF and find a 50% probability that the remaining heating will be $\ge 2.0$°F.

### Step 3: Expected Value (EV) Execution
Fetch the live limit order book parameters (`yes_ask`, `yes_bid`, `no_ask`, `no_bid`). Calculate the EV for every strike:
$EV_{YES} = (True\_Probability \times 100) - yes\_ask$
Execute orders on the strikes yielding the highest positive EV.

---

## 4. Missing Extra Features to Consider

1.  **High-Frequency Cloud Cover (GOES Satellite):** Integrating 5-minute GOES-16 satellite imagery to preemptively warn of incoming shadows.
2.  **Solar Radiation (W/m²):** If raw heating momentum is strong but solar radiation is dropping, the momentum is false and the temperature will soon stall.
3.  **Soil Moisture Anomaly:** Wet ground absorbs solar energy through evaporation. This can cause the HRRR to over-predict sensible heating.
4.  **Live Precipitation Flags:** Evaporative cooling from falling rain crashes surface temperatures almost instantly.
5.  **Historical Wind-Bias Matrix:** A static lookup table pairing the station's historical temperature bias relative to NBM/HRRR, conditioned on month, wind speed, and exact wind direction.