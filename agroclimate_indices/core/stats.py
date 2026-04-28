import numpy as np
import xarray as xr

def safe_divide(a, b):
    return xr.where(abs(b) > 1e-12, a / b, np.nan)

def minmax_score(x, qlow=0.05, qhigh=0.95, reverse=False):
    lo = x.quantile(qlow)
    hi = x.quantile(qhigh)
    y = safe_divide(x - lo, hi - lo)
    y = y.clip(0, 1)
    return 1 - y if reverse else y

def dayofyear_climatology(da, stat="mean", q=None):
    gb = da.groupby("time.dayofyear")
    if stat == "mean":
        return gb.mean("time")
    if stat == "std":
        return gb.std("time")
    if stat == "quantile":
        return gb.quantile(q, dim="time")
    raise ValueError(stat)

def month_climatology(da, stat="mean", q=None):
    gb = da.groupby("time.month")
    if stat == "mean":
        return gb.mean("time")
    if stat == "std":
        return gb.std("time")
    if stat == "quantile":
        return gb.quantile(q, dim="time")
    raise ValueError(stat)

def consecutive_max(condition):
    def _max_run(arr):
        m = 0
        r = 0
        for v in arr:
            if bool(v):
                r += 1
                m = max(m, r)
            else:
                r = 0
        return m
    return xr.apply_ufunc(
        _max_run, condition,
        input_core_dims=[["time"]],
        vectorize=True,
        dask="parallelized",
        output_dtypes=[np.int16],
    )

def consecutive_count(condition):
    return condition.sum("time")
