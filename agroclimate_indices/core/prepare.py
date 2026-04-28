from .io import find_var, read_yaml
from .units import (
    temperature_to_c, pressure_to_kpa, rh_to_percent,
    precipitation_to_mm_per_step, radiation_to_mj_per_m2_per_step
)

def harmonize_variables(ds, variable_map):
    """
    Maps raw variable names to standard names and converts units.
    """
    out = ds.copy()

    def put(target, da):
        if da is not None:
            out[target] = da

    vm = variable_map

    da = find_var(ds, vm["precipitation"]["candidates"], required=False)
    put("precipitation", precipitation_to_mm_per_step(da) if da is not None else None)

    for target in ["temperature_2m_max", "temperature_2m_min", "temperature_2m_mean"]:
        da = find_var(ds, vm[target]["candidates"], required=False)
        put(target, temperature_to_c(da) if da is not None else None)

    # If only mean temperature exists, keep max/min missing; if max/min exist but mean missing, derive mean.
    if "temperature_2m_mean" not in out and "temperature_2m_max" in out and "temperature_2m_min" in out:
        out["temperature_2m_mean"] = (out["temperature_2m_max"] + out["temperature_2m_min"]) / 2.0

    da = find_var(ds, vm["relative_humidity_2m"]["candidates"], required=False)
    put("relative_humidity_2m", rh_to_percent(da) if da is not None else None)

    da = find_var(ds, vm["wind_speed_10m"]["candidates"], required=False)
    put("wind_speed_10m", da if da is not None else None)

    da = find_var(ds, vm["shortwave_radiation"]["candidates"], required=False)
    put("shortwave_radiation", radiation_to_mj_per_m2_per_step(da) if da is not None else None)

    da = find_var(ds, vm["surface_pressure"]["candidates"], required=False)
    put("surface_pressure", pressure_to_kpa(da) if da is not None else None)

    da = find_var(ds, vm["soil_moisture"]["candidates"], required=False)
    put("soil_moisture", da if da is not None else None)

    da = find_var(ds, vm["elevation"]["candidates"], required=False)
    put("elevation", da if da is not None else None)

    keep = [v for v in [
        "precipitation", "temperature_2m_max", "temperature_2m_min",
        "temperature_2m_mean", "relative_humidity_2m", "wind_speed_10m",
        "shortwave_radiation", "surface_pressure", "soil_moisture", "elevation"
    ] if v in out]
    return out[keep]
