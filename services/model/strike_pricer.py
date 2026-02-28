"""Stage 4 — Strike Pricer.

Maps a monotonic quantile CDF onto Kalshi strike prices to produce
raw (uncalibrated) probabilities P_raw(CLI_High ≥ strike).

Algorithm (§8.4):
  The 7 quantile predictions define sample points on the *inverse* CDF:
      (q̂₀.₀₅, α=0.05), (q̂₀.₁₀, α=0.10), ..., (q̂₀.₉₅, α=0.95)

  For a strike at temperature S, compute required_delta = S − intraday_max.
  Find where required_delta falls in the sorted quantile predictions and
  interpolate linearly to get α_d = P(Y_t ≤ required_delta).
  Then P_raw(CLI_High ≥ S) = 1 − α_d.

Edge cases:
  required_delta ≤ 0          → P_raw = 1.0  (strike already exceeded)
  required_delta ≤ q̂₀.₀₅    → P_raw = 0.95  (clamp)
  required_delta ≥ q̂₀.₉₅    → P_raw = 0.05  (clamp)
"""

from __future__ import annotations

import numpy as np

from services.model.constants import QUANTILE_ALPHAS, STRIKE_P_FLOOR, STRIKE_P_CEIL


class StrikePricer:
    """Stage 4: CDF interpolation → P(CLI_High ≥ strike) per active Kalshi contract.

    This is *stateless* — no training required.  One instance per inference call.
    """

    def __init__(self, alphas: tuple[float, ...] = QUANTILE_ALPHAS):
        """
        Parameters
        ----------
        alphas : Quantile levels (must match the QuantileSuite used upstream).
                 Must be sorted ascending.
        """
        assert list(alphas) == sorted(alphas)
        self.alphas = np.array(alphas, dtype=np.float64)

    def price_strike(
        self,
        strike_f: float,
        custom_intraday_max_f: float,
        monotone_quantiles: dict[float, float],
    ) -> float:
        """Compute P_raw(CLI_High ≥ strike_f) for one strike.

        Parameters
        ----------
        strike_f : Strike temperature in °F.
        custom_intraday_max_f : Current running max temperature in °F.
        monotone_quantiles : {alpha → q̂_alpha} — output of MonotonicMapper.

        Returns
        -------
        float in [0, 1]. Uncalibrated raw probability.
        """
        required_delta = strike_f - custom_intraday_max_f

        # Strike already exceeded
        if required_delta <= 0.0:
            return 1.0

        q_values = np.array(
            [monotone_quantiles[alpha] for alpha in self.alphas],
            dtype=np.float64,
        )
        alphas = self.alphas

        # Clamp: below lowest quantile prediction
        if required_delta <= q_values[0]:
            return 1.0 - STRIKE_P_FLOOR   # = 0.95

        # Clamp: above highest quantile prediction
        if required_delta >= q_values[-1]:
            return 1.0 - STRIKE_P_CEIL    # = 0.05

        # Linear interpolation
        alpha_d = float(np.interp(required_delta, q_values, alphas))
        p_raw = 1.0 - alpha_d

        # Safety clamp (should be redundant given edge cases above)
        return float(np.clip(p_raw, 1.0 - STRIKE_P_CEIL, STRIKE_P_FLOOR + (1 - STRIKE_P_FLOOR)))

    def price_all_strikes(
        self,
        strikes: list[float],
        custom_intraday_max_f: float,
        monotone_quantiles: dict[float, float],
    ) -> dict[float, float]:
        """Compute P_raw for multiple strikes in one call.

        Returns dict: strike_f → P_raw.
        """
        return {
            s: self.price_strike(s, custom_intraday_max_f, monotone_quantiles)
            for s in strikes
        }
