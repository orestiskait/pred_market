"""Stage 3 — Monotonic Mapper.

Enforces the physical constraint that quantile predictions must be
non-decreasing:

    q̂₀.₀₅ ≤ q̂₀.₁₀ ≤ q̂₀.₂₅ ≤ q̂₀.₅₀ ≤ q̂₀.₇₅ ≤ q̂₀.₉₀ ≤ q̂₀.₉₅

XGBoost quantile models are trained independently and their raw outputs
can occasionally violate monotonicity (crossing quantiles).  This stage
corrects violations using the Pool Adjacent Violators (PAV) algorithm
(isotonic regression).

Also enforces: all quantiles ≥ 0.0  (remaining delta cannot be negative).
"""

from __future__ import annotations

import numpy as np

from services.model.constants import QUANTILE_ALPHAS


class MonotonicMapper:
    """Stage 3: enforce quantile monotonicity via PAV isotonic regression.

    This is a *stateless* transformer — no fitting required.  The same
    instance can be reused across all observations.
    """

    def __init__(self, alphas: tuple[float, ...] = QUANTILE_ALPHAS):
        """
        Parameters
        ----------
        alphas : Quantile levels in strictly ascending order.
        """
        # Validate we receive them sorted
        assert list(alphas) == sorted(alphas), "Alphas must be in ascending order."
        self.alphas = alphas

    def transform(self, raw_quantiles: dict[float, float]) -> dict[float, float]:
        """Apply PAV isotonic regression to a single observation's quantiles.

        Parameters
        ----------
        raw_quantiles : dict mapping alpha → raw quantile prediction value.
                        Must contain all alphas in self.alphas.

        Returns
        -------
        dict mapping alpha → monotonically non-decreasing prediction,
        with all values clipped to ≥ 0.
        """
        values = np.array(
            [raw_quantiles[alpha] for alpha in self.alphas],
            dtype=np.float64,
        )

        # PAV: pool adjacent violators
        monotone = _pool_adjacent_violators(values)

        # Clip to Y_MIN = 0 (remaining delta cannot be negative)
        monotone = np.maximum(monotone, 0.0)

        return {alpha: float(v) for alpha, v in zip(self.alphas, monotone)}

    def transform_batch(
        self, raw_quantiles: dict[float, np.ndarray]
    ) -> dict[float, np.ndarray]:
        """Apply PAV row-wise to a batch of predictions.

        Parameters
        ----------
        raw_quantiles : dict mapping alpha → 1-D array of shape (n_rows,).

        Returns
        -------
        dict mapping alpha → monotone 1-D array.
        """
        n = len(next(iter(raw_quantiles.values())))
        # Stack: shape (n_alphas, n_rows) → transpose → (n_rows, n_alphas)
        matrix = np.stack(
            [raw_quantiles[alpha] for alpha in self.alphas], axis=0
        ).T  # shape: (n_rows, n_alphas)

        result_matrix = np.zeros_like(matrix)
        for i in range(n):
            monotone = _pool_adjacent_violators(matrix[i])
            result_matrix[i] = np.maximum(monotone, 0.0)

        return {
            alpha: result_matrix[:, j]
            for j, alpha in enumerate(self.alphas)
        }


# ──────────────────────────────────────────────────────────────────────
# PAV algorithm (isotonic regression — non-decreasing)
# ──────────────────────────────────────────────────────────────────────

def _pool_adjacent_violators(values: np.ndarray) -> np.ndarray:
    """Pool Adjacent Violators algorithm for *non-decreasing* isotonic regression.

    O(n²) worst-case but n=7 here so this is negligible.  Implements the
    standard arithmetic-mean pool for ties / violations.

    Reference: Barlow et al. (1972), Statistical Inference under Order Restrictions.
    """
    n = len(values)
    # Work with groups: each group has a list of indices and their mean
    # We maintain (mean, count) tuples
    groups: list[tuple[float, int]] = [(float(v), 1) for v in values]

    changed = True
    while changed:
        changed = False
        i = 0
        merged: list[tuple[float, int]] = []
        while i < len(groups):
            if i + 1 < len(groups) and groups[i][0] > groups[i + 1][0]:
                # Violation detected — pool these two groups
                mean_i, cnt_i = groups[i]
                mean_j, cnt_j = groups[i + 1]
                pooled_mean = (mean_i * cnt_i + mean_j * cnt_j) / (cnt_i + cnt_j)
                merged.append((pooled_mean, cnt_i + cnt_j))
                i += 2
                changed = True
            else:
                merged.append(groups[i])
                i += 1
        groups = merged

    # Expand groups back to original length
    result = np.empty(n, dtype=np.float64)
    idx = 0
    for mean, cnt in groups:
        for _ in range(cnt):
            result[idx] = mean
            idx += 1

    return result
