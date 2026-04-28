import xarray as xr

def livestock_cold_stress_index(tmin, threshold_c=5.0):
    out = (tmin <= threshold_c).sum("time")
    out.name = "livestock_cold_stress_days"
    return out

def wind_chill_index(tmean, wind_ms):
    wind_kmh = wind_ms * 3.6
    out = 13.12 + 0.6215 * tmean - 11.37 * wind_kmh ** 0.16 + 0.3965 * tmean * wind_kmh ** 0.16
    out.name = "wind_chill_index"
    return out

def wind_chill_risk(wind_chill, threshold_c=0.0):
    out = (wind_chill <= threshold_c).sum("time")
    out.name = "wind_chill_risk_days"
    return out

def mud_wetness_risk(wet_spell_score, rainfall_excess_score):
    out = 0.5 * wet_spell_score + 0.5 * rainfall_excess_score
    out.name = "mud_wetness_risk"
    return out.clip(0, 1)

def vector_suitability_proxy(rainfall_suitability, humidity_suitability, temperature_suitability):
    out = (rainfall_suitability + humidity_suitability + temperature_suitability) / 3.0
    out.name = "vector_suitability_proxy"
    return out.clip(0, 1)

def surface_water_stress_proxy(rainfall_deficit, et0_excess, dry_spell_score):
    out = 0.40 * rainfall_deficit + 0.30 * et0_excess + 0.30 * dry_spell_score
    out.name = "surface_water_stress_proxy"
    return out.clip(0, 1)

def dust_dryness_risk(dry_spell_score, wind_score, low_humidity_score):
    out = 0.40 * dry_spell_score + 0.35 * wind_score + 0.25 * low_humidity_score
    out.name = "dust_dryness_risk"
    return out.clip(0, 1)
