import numpy as np
import xarray as xr
from scipy import stats
from agroclimate_indices.core.temporal import monthly_sum

def _fit_gamma_spi_1d(hist_values, target_values):
    hist_values = np.asarray(hist_values, dtype=float)
    target_values = np.asarray(target_values, dtype=float)
    h = hist_values[np.isfinite(hist_values)]
    if len(h) < 10:
        return np.full(target_values.shape, np.nan, dtype=float)

    zero_prob = np.mean(h <= 0)
    hp = h[h > 0]
    if len(hp) < 10:
        return np.full(target_values.shape, np.nan, dtype=float)

    shape, loc, scale = stats.gamma.fit(hp, floc=0)
    p = np.empty_like(target_values, dtype=float)

    for i, x in enumerate(target_values):
        if not np.isfinite(x):
            p[i] = np.nan
        elif x <= 0:
            prob = zero_prob
            p[i] = stats.norm.ppf(np.clip(prob, 1e-6, 1 - 1e-6))
        else:
            prob = zero_prob + (1 - zero_prob) * stats.gamma.cdf(x, shape, loc=0, scale=scale)
            p[i] = stats.norm.ppf(np.clip(prob, 1e-6, 1 - 1e-6))
    return p

def _fit_norm_1d(hist_values, target_values):
    h = np.asarray(hist_values, dtype=float)
    t = np.asarray(target_values, dtype=float)
    h = h[np.isfinite(h)]
    if len(h) < 10:
        return np.full(t.shape, np.nan, dtype=float)
    mu = np.nanmean(h)
    sd = np.nanstd(h)
    if sd <= 1e-12:
        return np.full(t.shape, np.nan, dtype=float)
    z = (t - mu) / sd
    p = stats.norm.cdf(z)
    return stats.norm.ppf(np.clip(p, 1e-6, 1 - 1e-6))

def _distribution_by_calendar_month(hist_series, forecast_series, fit_func, out_name):
    results = []
    for m in range(1, 13):
        hf = hist_series.where(hist_series["time.month"] == m, drop=True)
        ff = forecast_series.where(forecast_series["time.month"] == m, drop=True)
        nout = ff.sizes.get("time", 0)
        if nout == 0:
            continue

        arr = xr.apply_ufunc(
            fit_func,
            hf,
            ff,
            input_core_dims=[["time"], ["time"]],
            output_core_dims=[["forecast_time"]],
            exclude_dims=set(("time",)),
            vectorize=True,
            dask="parallelized",
            output_dtypes=[float],
            dask_gufunc_kwargs={"output_sizes": {"forecast_time": nout}},
        )
        arr = arr.assign_coords(forecast_time=ff.time.values).rename({"forecast_time": "time"})
        results.append(arr)

    if not results:
        raise ValueError(f"No forecast months available for {out_name}")

    out = xr.concat(results, dim="time").sortby("time")
    out.name = out_name
    out.attrs["units"] = "standardized"
    return out

def spi_gamma(forecast_daily_precip, hist_daily_precip, scale_months=3):
    f = monthly_sum(forecast_daily_precip).rolling(time=scale_months, min_periods=scale_months).sum()
    h = monthly_sum(hist_daily_precip).rolling(time=scale_months, min_periods=scale_months).sum()
    out = _distribution_by_calendar_month(h, f, _fit_gamma_spi_1d, f"spi_{scale_months}")
    out.attrs["method"] = "Gamma distribution fitted to historical precipitation by calendar month"
    return out

def spei_standardized(forecast_daily_p, forecast_daily_pet, hist_daily_p, hist_daily_pet, scale_months=3):
    fwb = monthly_sum(forecast_daily_p - forecast_daily_pet).rolling(time=scale_months, min_periods=scale_months).sum()
    hwb = monthly_sum(hist_daily_p - hist_daily_pet).rolling(time=scale_months, min_periods=scale_months).sum()
    out = _distribution_by_calendar_month(hwb, fwb, _fit_norm_1d, f"spei_{scale_months}")
    out.attrs["method"] = "Standardized climatic water balance fitted to historical P-ET0 by calendar month"
    return out
