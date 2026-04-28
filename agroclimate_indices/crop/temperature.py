import xarray as xr

def growing_degree_days(tmax, tmin, tbase_c=10.0):
    tmean = (tmax + tmin) / 2.0
    gdd = xr.where(tmean > tbase_c, tmean - tbase_c, 0).sum("time")
    gdd.name = "growing_degree_days"
    gdd.attrs["units"] = "degree-days"
    return gdd

def heat_stress_days(tmax, threshold_c=35.0):
    out = (tmax >= threshold_c).sum("time")
    out.name = "heat_stress_days"
    out.attrs["units"] = "days"
    return out

def cold_stress_days(tmin, threshold_c=5.0):
    out = (tmin <= threshold_c).sum("time")
    out.name = "cold_stress_days"
    out.attrs["units"] = "days"
    return out

def diurnal_temperature_range(tmax, tmin):
    out = (tmax - tmin).mean("time")
    out.name = "diurnal_temperature_range_mean"
    out.attrs["units"] = "degC"
    return out
