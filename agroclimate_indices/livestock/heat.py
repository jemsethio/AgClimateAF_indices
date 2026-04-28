import xarray as xr
from agroclimate_indices.core.stats import consecutive_max

def temperature_humidity_index(tmean, rh):
    rh_fraction = xr.where(rh > 1.5, rh / 100.0, rh).clip(0, 1)
    out = 0.8 * tmean + rh_fraction * (tmean - 14.4) + 46.4
    out.name = "temperature_humidity_index"
    return out

def thi_risk_class(thi):
    out = xr.where(
        thi < 68, 0,
        xr.where(thi < 72, 1,
        xr.where(thi < 78, 2,
        xr.where(thi < 84, 3, 4)))
    ).astype("int16")
    out.name = "thi_risk_class"
    out.attrs["classes"] = "0=low,1=mild,2=moderate,3=severe,4=emergency"
    return out

def heat_stress_duration(thi, threshold=72.0):
    out = (thi >= threshold).sum("time")
    out.name = "livestock_heat_stress_duration_days"
    return out

def max_heat_stress_spell(thi, threshold=72.0):
    out = consecutive_max(thi >= threshold)
    out.name = "max_livestock_heat_stress_spell"
    return out

def night_recovery_index(tmin, threshold=24.0):
    out = (tmin >= threshold).sum("time")
    out.name = "night_recovery_failure_days"
    return out

def water_demand_index(thi, baseline_water=40.0):
    m = xr.where(
        thi < 68, 1.0,
        xr.where(thi < 72, 1.1,
        xr.where(thi < 78, 1.25,
        xr.where(thi < 84, 1.5, 1.8)))
    )
    out = baseline_water * m
    out.name = "livestock_water_demand_index"
    out.attrs["units"] = "litre animal-1 day-1 relative"
    return out
