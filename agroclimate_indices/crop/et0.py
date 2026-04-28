import numpy as np
import xarray as xr

def fao56_et0_daily(tmax, tmin, tmean, rh, wind2m, rs_mj_m2_day, pressure_kpa):
    """
    FAO-56 Penman-Monteith daily ET0, simplified with measured incoming
    shortwave radiation as net-radiation driver approximation.

    ET0 =
    [0.408Δ(Rn-G) + γ 900/(T+273) u2(es-ea)]
    / [Δ + γ(1+0.34u2)]

    Output: mm/day
    """
    rh_frac = xr.where(rh > 1.5, rh / 100.0, rh).clip(0, 1)

    es_tmax = 0.6108 * np.exp((17.27 * tmax) / (tmax + 237.3))
    es_tmin = 0.6108 * np.exp((17.27 * tmin) / (tmin + 237.3))
    es = (es_tmax + es_tmin) / 2.0
    ea = es * rh_frac

    delta = 4098 * (0.6108 * np.exp((17.27 * tmean) / (tmean + 237.3))) / ((tmean + 237.3) ** 2)
    gamma = 0.000665 * pressure_kpa

    # Operational approximation: net radiation fraction from shortwave.
    # For full FAO56, replace this with net shortwave + net longwave calculation.
    rn = 0.77 * rs_mj_m2_day
    g = 0.0

    et0 = (
        0.408 * delta * (rn - g)
        + gamma * (900.0 / (tmean + 273.0)) * wind2m * (es - ea)
    ) / (delta + gamma * (1.0 + 0.34 * wind2m))

    et0 = et0.clip(min=0)
    et0.name = "et0"
    et0.attrs["units"] = "mm day-1"
    et0.attrs["method"] = "FAO-56 Penman-Monteith daily approximation"
    return et0
