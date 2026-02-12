# NWS Temperature Data Rules for Contract Resolution

This document summarizes the technical logic used by the National Weather Service (NWS) for recording and reporting temperature data, specifically for use in resolving weather-based contracts.

---

## 1. Designated Weather Stations
Official resolution is based on the **NWS Daily Climatological Report** from the following primary stations:

| City | Station Code | Location |
| :--- | :--- | :--- |
| **New York City** | **KNYC** | Central Park |
| **Miami** | **KMIA** | Miami International Airport |
| **Chicago** | **KMDW** | Chicago-Midway International Airport |
| **Denver** | **KDEN** | Denver International Airport |
| **Austin** | **KAUS** | Austin-Bergstrom International Airport |
| **Houston** | **KHOU** | Houston Hobby International Airport |
| **Philadelphia** | **KPHL** | Philadelphia International Airport |

---

## 2. Station Type Comparison

The NWS reports data from two different station types with distinct reporting behaviors.

### Hourly Stations
* **Recording Frequency:** Constantly records temperature; reports once per hour.
* **Precision:** Recorded to the nearest $0.1^{\circ}F$.
* **Processing:** $0.1^{\circ}F \rightarrow 0.1^{\circ}C \rightarrow$ back to Fahrenheit for reporting.
* **Accuracy:** Rounding error is typically minimal.
* **Daily High Logic:** Every 6–24 hours, the station sends the highest measured temperature to the NWS.
* **Rule of Thumb:** The official daily high is typically **greater than or equal to** the highest hourly reading shown in the time series.

### 5-Minute Stations
* **Recording Frequency:** Records 1-minute averages; reports a 5-minute average.
* **Rounding Process:** 5-minute average is rounded to the **nearest whole degree Fahrenheit**, then converted to the **nearest whole degree Celsius** for transmission.
* **Reporting Error:** The NWS reports Fahrenheit by converting the Celsius value back to Fahrenheit, ignoring the original Fahrenheit value.
* **Data Quality:** This introduces significant error (often $\pm 1^{\circ}F$ or more). 
* **Inference Rule:** One Celsius value can represent two different Fahrenheit temperatures. If these temperatures span different betting ranges, the data is ambiguous.

---

## 3. Calculation of the "Official High"
The NWS determines the final "High for the Day" using a method that avoids the errors found in the 5-minute time series.

1. **Source Data:** The NWS takes the highest **1-minute average** recorded by either the hourly or 5-minute station.
2. **Rounding Rule:** This raw value is rounded to the **nearest whole degree Fahrenheit**.
3. **Accuracy Advantage:** This value is **never converted to Celsius and back**, avoiding the conversion/rounding errors of the 5-minute time series.
4. **Peak vs. Average:** Because the high is based on 1-minute peaks rather than 5-minute averages, the official high can be higher than any value appearing in the 5-minute time series.

---

## 4. Operational Summary
* **Time Series Limitations:** Near-real-time data (especially 5-minute stations) is an approximation. The conversion loop ($F \rightarrow C \rightarrow F$) makes reported values unreliable for precision betting.
* **Hidden Data:** The official high uses raw 1-minute data that is not always visible in the rounded 5-minute or hourly averages.