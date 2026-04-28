import numpy as np
import xarray as xr

def attrs_unit(da: xr.DataArray) -> str:
    return str(da.attrs.get("units", "")).lower().strip()

def temperature_to_c(da: xr.DataArray) -> xr.DataArray:
    unit = attrs_unit(da)
    if "k" == unit or "kelvin" in unit or float(da.mean(skipna=True)) > 150:
        out = da - 273.15
    else:
        out = da
    out.attrs["units"] = "degC"
    return out

def pressure_to_kpa(da: xr.DataArray) -> xr.DataArray:
    unit = attrs_unit(da)
    if unit in ["pa", "pascal", "pascals"] or float(da.mean(skipna=True)) > 2000:
        out = da / 1000.0
    elif unit in ["hpa", "mbar", "millibar"]:
        out = da / 10.0
    else:
        out = da
    out.attrs["units"] = "kPa"
    return out

def rh_to_percent(da: xr.DataArray) -> xr.DataArray:
    unit = attrs_unit(da)
    if "fraction" in unit or float(da.max(skipna=True)) <= 1.5:
        out = da * 100.0
    else:
        out = da
    out = out.clip(0, 100)
    out.attrs["units"] = "%"
    return out

def precipitation_to_mm_per_step(da: xr.DataArray) -> xr.DataArray:
    """
    Converts common precipitation forms to mm per native timestep.
    Handles:
      - m accumulated per step -> mm
      - kg m-2 s-1 or mm/s rate -> mm per timestep
      - mm already -> unchanged
    """
    unit = attrs_unit(da)
    if "kg" in unit and "s-1" in unit:
        dt_seconds = infer_timestep_seconds(da)
        out = da * dt_seconds
    elif unit in ["m", "meter", "metre", "meters", "metres"]:
        out = da * 1000.0
    elif "mm" in unit:
        out = da
    else:
        # ERA5 tp often uses metres even if attrs are missing; detect very small values.
        mean = float(da.mean(skipna=True))
        out = da * 1000.0 if mean < 0.05 else da
    out = out.where(out >= 0, 0)
    out.attrs["units"] = "mm"
    return out

def radiation_to_mj_per_m2_per_step(da: xr.DataArray) -> xr.DataArray:
    """
    Converts radiation to MJ m-2 per native timestep.
    ERA5 ssrd is commonly J m-2 accumulated.
    W m-2 is converted by multiplying timestep seconds.
    """
    unit = attrs_unit(da)
    if "j" in unit and "m" in unit:
        out = da / 1_000_000.0
    elif "w" in unit and "m" in unit:
        out = da * infer_timestep_seconds(da) / 1_000_000.0
    else:
        # If values are very large, assume J/m2.
        mean = float(da.mean(skipna=True))
        out = da / 1_000_000.0 if mean > 1000 else da
    out.attrs["units"] = "MJ m-2"
    return out

def infer_timestep_seconds(da: xr.DataArray) -> float:
    times = da["time"].values
    if len(times) < 2:
        return 86400.0
    diffs = np.diff(times).astype("timedelta64[s]").astype(float)
    return float(np.nanmedian(diffs))
