import xarray as xr
from .temporal import monthly_sum, monthly_mean, rolling_sum
from .stats import dayofyear_climatology, month_climatology

def build_climatology(hist_daily: xr.Dataset, baseline_start: str, baseline_end: str) -> xr.Dataset:
    """
    Builds reference climatologies from historical daily data.
    No external precomputed climatology is required.
    """
    h = hist_daily.sel(time=slice(baseline_start, baseline_end))
    clim = xr.Dataset()

    if "precipitation" in h:
        p = h["precipitation"]
        clim["precip_doy_mean"] = dayofyear_climatology(p, "mean")
        clim["precip_doy_p33"] = dayofyear_climatology(p, "quantile", 0.33)
        clim["precip_doy_p66"] = dayofyear_climatology(p, "quantile", 0.66)
        clim["precip_doy_p95"] = dayofyear_climatology(p, "quantile", 0.95)

        pm = monthly_sum(p)
        clim["precip_month_mean"] = month_climatology(pm, "mean")
        clim["precip_month_p33"] = month_climatology(pm, "quantile", 0.33)
        clim["precip_month_p66"] = month_climatology(pm, "quantile", 0.66)

        for w in [10, 30, 90]:
            clim[f"precip_roll{w}_doy_mean"] = dayofyear_climatology(rolling_sum(p, w), "mean")

    for var in ["temperature_2m_mean", "temperature_2m_max", "temperature_2m_min",
                "relative_humidity_2m", "wind_speed_10m", "shortwave_radiation",
                "surface_pressure", "soil_moisture", "et0"]:
        if var in h:
            da = h[var]
            clim[f"{var}_doy_mean"] = dayofyear_climatology(da, "mean")
            clim[f"{var}_month_mean"] = month_climatology(monthly_mean(da), "mean")

    if "soil_moisture" in h:
        clim["soil_moisture_doy_p20"] = dayofyear_climatology(h["soil_moisture"], "quantile", 0.20)

    return clim
