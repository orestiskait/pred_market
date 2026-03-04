"""Central temperature unit conversions.

All Fahrenheit ↔ Celsius (and Kelvin) conversions should go through these
functions. Do not use inline calculations (e.g. c * 9/5 + 32) elsewhere.
"""

from __future__ import annotations

# Scale factors for consistency
_C_TO_F_SCALE = 9.0 / 5.0
_C_TO_F_OFFSET = 32.0
_K_TO_F_OFFSET = 459.67


def celsius_to_fahrenheit(c: float | None) -> float | None:
    """Convert Celsius to Fahrenheit. Returns None if input is None."""
    if c is None:
        return None
    return c * _C_TO_F_SCALE + _C_TO_F_OFFSET


def fahrenheit_to_celsius(f: float | None) -> float | None:
    """Convert Fahrenheit to Celsius. Returns None if input is None."""
    if f is None:
        return None
    return (f - _C_TO_F_OFFSET) / _C_TO_F_SCALE


def kelvin_to_fahrenheit(k: float) -> float:
    """Convert Kelvin to Fahrenheit."""
    return k * _C_TO_F_SCALE - _K_TO_F_OFFSET


def kelvin_to_celsius(k: float) -> float:
    """Convert Kelvin to Celsius."""
    return k - 273.15


def celsius_delta_to_fahrenheit_delta(dc: float) -> float:
    """Convert a temperature difference in °C to °F. (Δ°F = Δ°C × 9/5)"""
    return dc * _C_TO_F_SCALE
