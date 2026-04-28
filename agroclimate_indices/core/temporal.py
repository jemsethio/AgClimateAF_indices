import xarray as xr

def aggregate_to_daily(ds: xr.Dataset) -> xr.Dataset:
    """
    Aggregates hourly/3-hourly/6-hourly data to daily variables.
    Input variables must already be converted to per-step units where relevant.

    If only instantaneous/mean 2m temperature is available, it derives:
      daily Tmax = max of sub-daily t2m
      daily Tmin = min of sub-daily t2m
      daily Tmean = mean of sub-daily t2m
    """
    out = xr.Dataset(coords={k: v for k, v in ds.coords.items() if k != "time"})

    if "precipitation" in ds:
        out["precipitation"] = ds["precipitation"].resample(time="1D").sum()

    if "temperature_2m_max" in ds:
        out["temperature_2m_max"] = ds["temperature_2m_max"].resample(time="1D").max()
    elif "temperature_2m_mean" in ds:
        out["temperature_2m_max"] = ds["temperature_2m_mean"].resample(time="1D").max()

    if "temperature_2m_min" in ds:
        out["temperature_2m_min"] = ds["temperature_2m_min"].resample(time="1D").min()
    elif "temperature_2m_mean" in ds:
        out["temperature_2m_min"] = ds["temperature_2m_mean"].resample(time="1D").min()

    if "temperature_2m_mean" in ds:
        out["temperature_2m_mean"] = ds["temperature_2m_mean"].resample(time="1D").mean()
    elif "temperature_2m_max" in ds and "temperature_2m_min" in ds:
        out["temperature_2m_mean"] = (
            out["temperature_2m_max"] + out["temperature_2m_min"]
        ) / 2.0

    if "relative_humidity_2m" in ds:
        out["relative_humidity_2m"] = ds["relative_humidity_2m"].resample(time="1D").mean()
    if "wind_speed_10m" in ds:
        out["wind_speed_10m"] = ds["wind_speed_10m"].resample(time="1D").mean()
        out["wind_speed_10m_max"] = ds["wind_speed_10m"].resample(time="1D").max()
    if "shortwave_radiation" in ds:
        out["shortwave_radiation"] = ds["shortwave_radiation"].resample(time="1D").sum()
    if "surface_pressure" in ds:
        out["surface_pressure"] = ds["surface_pressure"].resample(time="1D").mean()
    if "soil_moisture" in ds:
        out["soil_moisture"] = ds["soil_moisture"].resample(time="1D").mean()
    if "elevation" in ds:
        out["elevation"] = ds["elevation"]

    return out

def monthly_sum(da):
    return da.resample(time="MS").sum()

def monthly_mean(da):
    return da.resample(time="MS").mean()

def rolling_sum(da, window):
    return da.rolling(time=window, min_periods=window).sum()

def rolling_mean(da, window):
    return da.rolling(time=window, min_periods=window).mean()
