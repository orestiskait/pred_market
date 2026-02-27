# Kalshi Daily High Temperature Quantitative Trading Model

## System Architecture Overview
The system is divided into two decoupled engines:
1. **The Weather Brain (ML Model):** Predicts exactly how much hotter the physical atmosphere will get before the climate day ends.
2. **The Execution Brain (Trading Logic):** Maps the Weather Brain's physical forecast against every active Kalshi strike contract to find mispriced expected value (EV) and execute trades.

---

## 1. The Response Variable (Target Label)
Instead of predicting the absolute peak temperature, the model predicts the **Remaining Heating Delta**. This solves the issue of non-stationarity and natively handles post-peak data (when the remaining delta converges to zero).

* **Definition**: `Y = Final_CLI_High - Custom_Intraday_Max_t`
* **Historical Target Construction**: Extract the `Final_CLI_High` from the historical NWS Daily Climate Report batch data. The `Custom_Intraday_Max_t` is the simulated high-water mark calculated up to the specific point in time `t` for that historical day.
* **Model Type**: **Quantile Regression** (e.g., LightGBM with Pinball Loss). Instead of predicting a single mean value, the model outputs an array of percentiles (e.g., 10th, 50th, 90th percentile) for the delta, effectively generating a continuous Probability Density Function (PDF).

---

## 2. The Weather Brain: Features & Time Alignment
Time alignment is the most critical guardrail in backtesting. The features must strictly respect processing and publishing latencies to avoid look-ahead bias. Kalshi strikes are strictly excluded from these inputs.



### A. The "State of the Day" Envelope (1-Minute Frequency)
*Alignment: Instantaneous. Pulled from live Wethr/Synoptic 1-minute ASOS streams.*

* **`custom_intraday_max`**: The bedrock of your system. This is the maximum of:
    1. Your in-house 5-minute simple moving average of the raw 1-minute Celsius data, converted to Fahrenheit and rounded.
    2. The highest precise 0.1°C temperature parsed from any METAR T-group remarks received so far today.
    *Rule: You must hard-reset this calculation at exactly 00:00 Local Standard Time (LST), ignoring Daylight Saving Time completely.*
* **`raw_1min_max_f`**: The absolute highest single 1-minute reading observed today.
* **`estimation_spread`**: `raw_1min_max_f - custom_intraday_max`. Captures the uncertainty/volatility around the ASOS smoothing algorithm.

### B. Micro-Momentum (1-Minute Frequency)
*Alignment: Instantaneous.*

* **`velocity_15m`**: Current 1-minute temperature minus the 1-minute temperature 15 minutes ago.
* **`acceleration_15m`**: The current `velocity_15m` minus the `velocity_15m` from 5 minutes ago. (Negative values indicate flattening heating curves).
* **`celsius_rounding_danger`**: The distance from the current 1-minute Celsius temperature to the nearest 0.1°C boundary that will trigger the ASOS firmware to round up to the next whole Fahrenheit degree.

### C. NWP Baseline, Quantiles & Calibration (Hourly / 15-Minute)
*Alignment: Strictly lagged to reflect processing delays (~1.5 hours for HRRR/NBM, ~29 mins for RTMA-RU).*

* **`hrrr_expected_remaining_delta`**: The highest 15-minute temperature forecasted in the remainder of the climate day from the HRRR `subh` product, minus the `custom_intraday_max`. 
* **`hrrr_current_error_f`**: The high-precision METAR T-group temperature minus the HRRR forecast valid for that exact observation hour.
* **`rtma_current_divergence`**: Current 1-minute temperature minus the latest available RTMA-RU analysis. 
* **`neighbor_lake_gradient`**: The high-precision METAR temperature difference between your target station (e.g., KMDW) and a coastal/adjacent station (e.g., KORD). Used to detect approaching localized fronts like lake breezes.

* **NBM Probabilistic Quantiles (The Shrinking Horizon)**: Because the NBM is a statistically post-processed blend, it provides probabilistic grids. Since Kalshi settles on the *daily high*, you must calculate the maximum expected value over the shrinking window of remaining hours in the day:
    * **`nbm_forecast_p90`**: The **maximum** of all remaining hourly 90th-percentile NBM forecasts until midnight LST. This represents the "reasonable worst-case heat" scenario.
    * **`nbm_forecast_p10`**: The **maximum** of all remaining hourly 10th-percentile NBM forecasts until midnight LST. This represents the "reasonable worst-case cold" scenario.
    * **`nbm_spread`**: `nbm_forecast_p90 - nbm_forecast_p10`. A narrow spread signals high model consensus; a wide spread signals atmospheric chaos, explicitly instructing your ML model to widen its own probability distributions.

> **Note on RRFS (Rapid Refresh Forecast System)**: RRFS is NOAA's next-generation model designed to replace HRRR. As of early 2026, data generation is paused in a pre-operational state, and it only produces hourly output without the 15-minute granularity of the HRRR `subh` product. Exclude RRFS from live ML inference for now, but maintain the ingestion pipeline code to future-proof the architecture.

---

## 3. The Execution Brain: Mapping to Strikes & Probabilities


Once your Weather Brain predicts the Probability Density Function (PDF) of the remaining heating delta (`Delta_remaining`), the Execution Brain loops through every active Kalshi contract in real-time.

### Step 1: Define the Required Delta per Kalshi Contract
Kalshi generates a daily series with discrete strikes (e.g., 73°F, 74°F, 75°F). For *each* active contract, calculate the physical heat required to trigger a YES payout:
`Required_Delta = Strike_Temperature - custom_intraday_max`

### Step 2: Extract the Cumulative Probability
Query your ML model's Quantile Regression output to find the probability that the actual heating will be greater than or equal to the `Required_Delta`.
* *Example*: `custom_intraday_max` is 72°F. You evaluate the "High above 74°F" contract. 
* The `Required_Delta` is 2.0°F. You query the model's CDF and find a 50% probability that the remaining heating will be >= 2.0°F.
* Therefore, your True Probability for the 74°F contract is 0.50.

### Step 3: Expected Value (EV) Execution
Fetch the live limit order book parameters (`yes_ask`, `yes_bid`, `no_ask`, `no_bid`) from your market snapshots. Calculate the EV for every strike simultaneously:
`EV_YES = (True_Probability * 100) - yes_ask`
Execute orders on the strikes yielding the highest positive EV, provided the bid-ask spread is tight enough to warrant the risk.

---

## 4. Missing Extra Features to Consider

The data sources currently detailed in your pipeline are excellent for temperature tracking and model calibration, but they lack explicit physical forcing inputs. To squeeze out additional edge, consider adding these external features to the Weather Brain:

1. **High-Frequency Cloud Cover (GOES Satellite):** METAR reports cloud cover (e.g., CLR, SCT, OVC), but only once an hour. Integrating 5-minute GOES-16 satellite imagery can preemptively warn your model of incoming shadows.
2. **Solar Radiation (W/m²):** Closely related to cloud cover. If raw heating momentum is strong, but the pyranometer shows solar radiation dropping, the momentum is false and the temperature will soon stall.
3. **Soil Moisture Anomaly:** Wet ground absorbs solar energy through evaporation rather than sensible heating. If the HRRR expects 80°F, but there was a localized flash flood the day before that the HRRR failed to assimilate, the air will be cooler and more humid than modeled.
4. **Live Precipitation Flags:** Evaporative cooling from falling rain crashes surface temperatures almost instantly. If the ASOS or a nearby radar detects precip, the probability of further heating essentially drops to near-zero for that hour.
5. **Historical Wind-Bias Matrix:** A static lookup table pairing the station's historical temperature bias relative to NBM/HRRR, conditioned explicitly on month, wind speed, and exact wind direction degrees.