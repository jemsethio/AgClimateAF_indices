def solar_radiation_anomaly(srad_total, srad_clim_total):
    out = srad_total - srad_clim_total
    out.name = "solar_radiation_anomaly"
    return out

def wind_risk_days(wind_speed, threshold_ms=8.0):
    out = (wind_speed >= threshold_ms).sum("time")
    out.name = "wind_risk_days"
    out.attrs["units"] = "days"
    return out
