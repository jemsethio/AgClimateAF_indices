from pathlib import Path
import yaml
import xarray as xr

def open_netcdf(path: str | Path, chunks=None) -> xr.Dataset:
    """
    Open NetCDF with optional Dask chunks.
    If Dask is not installed, automatically falls back to normal xarray loading.
    """
    path = Path(path)
    if not path.exists():
        raise FileNotFoundError(path)
    try:
        ds = xr.open_dataset(path, chunks=chunks) if chunks else xr.open_dataset(path)
    except ImportError as e:
        if "dask" in str(e).lower():
            ds = xr.open_dataset(path)
        else:
            raise
    return standardize_dims(ds)

def standardize_dims(ds: xr.Dataset) -> xr.Dataset:
    ren = {}
    for old, new in {
        "latitude": "lat",
        "longitude": "lon",
        "valid_time": "time",
        "forecast_time": "time",
    }.items():
        if old in ds.dims or old in ds.coords:
            ren[old] = new
    ds = ds.rename(ren)
    if "time" not in ds.dims and "time" not in ds.coords:
        raise ValueError("Dataset must include a time coordinate/dimension.")
    return ds.sortby("time")

def read_yaml(path: str | Path) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)

def find_var(ds: xr.Dataset, candidates: list[str], required=True):
    for name in candidates:
        if name in ds.data_vars:
            return ds[name]
    if required:
        raise KeyError(f"None of these variables found: {candidates}")
    return None

def select_baseline(ds: xr.Dataset, start: str, end: str) -> xr.Dataset:
    return ds.sel(time=slice(start, end))
