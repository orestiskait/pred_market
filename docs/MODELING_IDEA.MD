# Kalshi Daily High Temperature Quantitative Trading Model

## 1. The Response Variable (Target Label)
Instead of predicting the absolute peak temperature, the model predicts the **Remaining Heating Delta**. This solves the issue of non-stationarity and natively handles post-peak data (when the remaining delta converges to zero).

* **Definition**: Y = Final_CLI_High - Custom_Intraday_Max_t
* **Historical Target Construction**: You extract the `Final_CLI_High` from the historical NWS Daily Climate Report batch data. The Custom_Intraday_Max_t is the simulated high-water mark calculated up to the specific point in time 't' for that historical day.
* **Model Type**: **Quantile Regression** (e.g., LightGBM with Pinball Loss). Instead of predicting a single mean value, the model outputs an array of percentiles (e.g., 1st to 99th percentile) for the delta, effectively generating a continuous Probability Density Function (PDF).

---

## 2. Feature Engineering & Time Alignment
Time alignment is the most critical guardrail in backtesting. If you train a model at 14:00 using an NWP forecast that was physically impossible to download until 14:45, your model will suffer from look-ahead bias and fail in production.

### A. The "State of the Day" Envelope (1-Minute Frequency)
*Alignment: Instantaneous. Pulled from live Wethr/Synoptic 1-minute ASOS streams.*

* **`custom_intraday_max`**: The bedrock of your system. This is the maximum of:
    1. Your in-house 5-minute simple moving average of the raw 1-minute Celsius data, converted to Fahrenheit and rounded.
    2. The highest precise 0.1°C temperature parsed from any METAR T-group remarks received so far today.
    *Rule: You must hard-reset this calculation at exactly 00:00 Local Standard Time (LST), ignoring Daylight Saving Time completely.*
* **`raw_1min_max_f`**: The absolute highest single 1-minute reading observed today.
* **`estimation_spread`**: raw_1min_max_f - custom_intraday_max. Captures the uncertainty/volatility around the ASOS smoothing algorithm.
* **`dist_to_strike`**: Kalshi_Strike - custom_intraday_max. Calculated dynamically for every active contract in the series.

### B. Micro-Momentum (1-Minute Frequency)
*Alignment: Instantaneous.*

* **`velocity_15m`**: Current 1-minute temperature minus the 1-minute temperature 15 minutes ago.
* **`acceleration_15m`**: The current `velocity_15m` minus the `velocity_15m` from 5 minutes ago. (Negative values indicate flattening heating curves).
* **`celsius_rounding_danger`**: The distance from the current 1-minute Celsius temperature to the nearest 0.1°C boundary that will trigger the ASOS firmware to round up to the next whole Fahrenheit degree.

### C. NWP Baseline & Calibration (Hourly / 15-Minute)
*Alignment: Strictly lagged to reflect processing delays.*

* **`hrrr_expected_remaining_delta`**: The highest 15-minute temperature forecasted in the remainder of the climate day, minus the `custom_intraday_max`. **Crucial:** You must use the HRRR cycle initialized at least 1.5 hours prior to the current time to simulate real-world availability.
* **`hrrr_current_error_f`**: The high-precision METAR T-group temperature minus the HRRR forecast valid for that exact observation hour.
* **`error_trend_3h`**: The rate of change in the `hrrr_current_error_f` over the last 3 hours.
* **`rtma_current_divergence`**: Current 1-minute temperature minus the latest available RTMA-RU analysis. Because RTMA-RU has a low latency (~29 minutes), it acts as a bridge between the live observation and the 1.5-hour lagged HRRR.
* **`neighbor_lake_gradient`**: The high-precision METAR temperature difference between your target station (e.g., KMDW) and a coastal/adjacent station (e.g., KORD). Used to detect approaching localized fronts like lake breezes.

---

## 3. Mapping Response to Strikes & Probabilities
Once your model predicts the distribution of the remaining heating delta (Delta_remaining), you must translate that into discrete Kalshi contract prices.

### Step 1: Define the Required Delta per Strike
For any given Kalshi strike, determine how much the temperature must rise to trigger a YES.
Required_Delta = Strike - custom_intraday_max

### Step 2: Extract the Cumulative Probability
Query your model's Quantile Regression output to find the probability that the actual delta will be greater than or equal to the Required_Delta.
* *Example*: The current high is 73°F. You are evaluating the "High above 74°F" contract. The Required_Delta is 1.0°F. You look at your model's CDF and see a 65% probability that the remaining heating will be >= 1.0°F.
* Therefore, your True Probability for the 74°F contract is 0.65.

### Step 3: Expected Value (EV) Execution
Fetch the live limit order book parameters (`yes_ask`, `yes_bid`, `no_ask`, `no_bid`) from your market snapshots.
EV_YES = (True_Probability * 100) - yes_ask
Execute orders on the strikes yielding the highest positive EV, provided the bid-ask spread is tight enough to warrant the risk.

---

## 4. Missing Extra Features to Consider

The data sources currently detailed in your pipeline are excellent for temperature tracking and model calibration, but they lack explicit physical forcing inputs. To squeeze out additional edge, consider adding these external features:

1. **High-Frequency Cloud Cover (GOES Satellite):** METAR reports cloud cover (e.g., CLR, SCT, OVC), but only once an hour. A stray cloud shading the ASOS sensor for 15 minutes at 2:00 PM can permanently cap the daily high. Integrating 5-minute GOES-16 satellite visible/infrared imagery can preemptively warn your model of incoming shadows.
2. **Solar Radiation (W/m²):** Closely related to cloud cover. If your raw heating momentum is strong, but the pyranometer shows solar radiation dropping, the momentum is false and the temperature will soon stall.
3. **Soil Moisture Anomaly:** Wet ground absorbs solar energy through evaporation rather than sensible heating (raising the air temperature). If the HRRR expects 80°F, but there was a localized flash flood the day before that the HRRR failed to assimilate, the air will be cooler and more humid than modeled.
4. **Live Precipitation Flags:** Evaporative cooling from falling rain crashes surface temperatures almost instantly. If the ASOS or a nearby radar detects precip, the probability of further heating essentially drops to near-zero for that hour.
5. **Historical Wind-Bias Matrix:** A static lookup table pairing the station's historical temperature bias relative to NBM/HRRR, conditioned explicitly on month, wind speed, and exact wind direction degrees.