import numpy as np
import pandas as pd
import xarray as xr
from agroclimate_indices.core.stats import consecutive_max

def _doy_array(time):
    return xr.DataArray(pd.to_datetime(time.values).dayofyear, coords={"time": time}, dims=["time"])

def onset_of_rainy_season(
    precip,
    onset_total_mm=25.0,
    onset_window_days=7,
    search_start_doy=60,
    search_end_doy=250,
    dry_day_mm=1.0,
    false_start_check_days=30,
    false_start_dry_spell_days=7,
):
    doy = _doy_array(precip.time)
    p = precip.where((doy >= search_start_doy) & (doy <= search_end_doy))
    rolling = p.rolling(time=onset_window_days, min_periods=onset_window_days).sum()
    candidate = rolling >= onset_total_mm

    # Filter candidates that are followed by damaging dry spells.
    valid = []
    for i in range(precip.sizes["time"]):
        if i + false_start_check_days >= precip.sizes["time"]:
            valid.append(candidate.isel(time=i) & False)
            continue
        following = precip.isel(time=slice(i + 1, i + 1 + false_start_check_days))
        maxdry = consecutive_max(following < dry_day_mm)
        valid.append(candidate.isel(time=i) & (maxdry < false_start_dry_spell_days))
    valid = xr.concat(valid, dim=precip.time)

    first_idx = valid.argmax("time")
    has_onset = valid.any("time")
    out = doy.isel(time=first_idx)
    out = xr.where(has_onset, out, np.nan)
    out.name = "onset_doy"
    return out

def false_start_risk(
    precip,
    onset_doy,
    check_days=30,
    dry_day_mm=1.0,
    dry_spell_days=7,
):
    doy = _doy_array(precip.time)
    mask = (doy > onset_doy) & (doy <= onset_doy + check_days)
    maxdry = consecutive_max((precip < dry_day_mm).where(mask, False))
    out = xr.where(maxdry >= dry_spell_days, 1, 0).astype("int16")
    out.name = "false_start_risk"
    return out

def cessation_date(
    precip,
    et0,
    search_start_doy=220,
    water_balance_threshold_mm=0.0,
):
    doy = _doy_array(precip.time)
    wb = precip - et0
    supportive = (wb >= water_balance_threshold_mm) & (doy >= search_start_doy)
    rev = supportive.isel(time=slice(None, None, -1))
    last_from_end = rev.argmax("time")
    n = precip.sizes["time"]
    idx = n - 1 - last_from_end
    has = supportive.any("time")
    out = doy.isel(time=idx)
    out = xr.where(has, out, np.nan)
    out.name = "cessation_doy"
    return out

def length_of_growing_period(onset_doy, cessation_doy):
    out = cessation_doy - onset_doy
    out = xr.where(out >= 0, out, np.nan)
    out.name = "length_of_growing_period"
    out.attrs["units"] = "days"
    return out
