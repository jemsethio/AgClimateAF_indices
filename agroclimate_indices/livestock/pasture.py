import xarray as xr
from agroclimate_indices.core.temporal import rolling_sum
from agroclimate_indices.core.stats import safe_divide

def pasture_rainfall_index(precip, hist_precip, window=30):
    p = rolling_sum(precip, window)
    hroll = rolling_sum(hist_precip, window)
    hmean = hroll.groupby("time.dayofyear").mean("time")

    doy = p["time"].dt.dayofyear
    hmatched = hmean.sel(dayofyear=doy).drop_vars("dayofyear")
    hmatched = hmatched.assign_coords(time=p.time)

    out = 100 * safe_divide(p - hmatched, hmatched)
    out.name = f"pasture_rainfall_index_{window}d"
    out.attrs["units"] = "%"
    return out

def temperature_suitability(tmean, tmin=10.0, topt1=20.0, topt2=30.0, tmax=40.0):
    lower = (tmean - tmin) / (topt1 - tmin)
    upper = (tmax - tmean) / (tmax - topt2)
    score = xr.where(tmean < topt1, lower, xr.where(tmean <= topt2, 1, upper))
    return score.clip(0, 1)

def forage_growth_proxy(rainfall_score, soil_moisture_score, temp_suitability):
    out = 0.45 * rainfall_score + 0.35 * soil_moisture_score + 0.20 * temp_suitability
    out.name = "forage_growth_proxy"
    return out.clip(0, 1)

def pasture_drought_index(rainfall_deficit, et0_excess, soil_moisture_deficit):
    out = 0.40 * rainfall_deficit + 0.30 * et0_excess + 0.30 * soil_moisture_deficit
    out.name = "pasture_drought_index"
    return out.clip(0, 1)
