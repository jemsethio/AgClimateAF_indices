import xarray as xr
from agroclimate_indices.core.stats import safe_divide, consecutive_count, consecutive_max

def rainfall_total(precip):
    out = precip.sum("time")
    out.name = "rainfall_total"
    out.attrs["units"] = "mm"
    return out

def rainfall_anomaly(p_total, clim_total):
    out = p_total - clim_total
    out.name = "rainfall_anomaly"
    out.attrs["units"] = "mm"
    return out

def rainfall_anomaly_percent(p_total, clim_total):
    out = 100 * safe_divide(p_total - clim_total, clim_total)
    out.name = "rainfall_anomaly_percent"
    out.attrs["units"] = "%"
    return out

def tercile_class(p_total, p33, p66):
    out = xr.where(p_total < p33, 0, xr.where(p_total > p66, 2, 1)).astype("int16")
    out.name = "tercile_class"
    out.attrs["classes"] = "0=BN,1=N,2=AN"
    return out

def dry_spell_days(precip, dry_day_mm=1.0):
    return consecutive_count(precip < dry_day_mm).rename("dry_spell_days")

def max_consecutive_dry_spell(precip, dry_day_mm=1.0):
    return consecutive_max(precip < dry_day_mm).rename("max_consecutive_dry_spell")

def wet_spell_days(precip, wet_day_mm=1.0):
    return consecutive_count(precip >= wet_day_mm).rename("wet_spell_days")

def max_consecutive_wet_spell(precip, wet_day_mm=1.0):
    return consecutive_max(precip >= wet_day_mm).rename("max_consecutive_wet_spell")

def extreme_rainfall_days(precip, threshold):
    return (precip >= threshold).sum("time")
