"""
Synthetic NetCDF smoke test.

This creates small 6-hourly historical and forecast NetCDF files,
runs the pipeline, and checks that all listed crop, livestock, and
integrated indices are present in the output NetCDF.

Run from package root:

python examples/smoke_test_synthetic_netcdf.py
"""
from pathlib import Path
import subprocess
import sys
import os
import shutil
import numpy as np
import pandas as pd
import xarray as xr

ROOT = Path(__file__).resolve().parents[1]
TMP = ROOT / "_smoke_test_data"
if TMP.exists():
    shutil.rmtree(TMP)
TMP.mkdir()

lat = np.array([0.0])
lon = np.array([30.0])
hist_time = pd.date_range("1991-01-01", "1992-12-31 18:00", freq="6h")
fcst_time = pd.date_range("1993-06-01", "1993-06-30 18:00", freq="6h")

def make_ds(time, seed):
    rng = np.random.default_rng(seed)
    nt = len(time)
    doy = pd.DatetimeIndex(time).dayofyear.values
    hour = pd.DatetimeIndex(time).hour.values

    t_c = 24 + 5*np.sin(2*np.pi*(doy-80)/365) + 4*np.sin(2*np.pi*(hour-6)/24) + rng.normal(0,0.8,nt)
    rh = np.clip(65 + 10*np.sin(2*np.pi*(doy+30)/365) + rng.normal(0,4,nt), 25, 98)
    rain_mm = rng.gamma(1.2, 3.0, nt) * (rng.random(nt) < 0.35)
    wind = np.clip(rng.normal(4,1.0,nt), 0.2, 10)
    ssrd = np.clip(2.5e6 + 1.0e6*np.sin(2*np.pi*(hour-6)/24) + rng.normal(0,1e5,nt), 0, None)
    sp = np.full(nt, 90000.0)
    sm = np.clip(0.25 + 0.05*np.sin(2*np.pi*(doy+20)/365) + rng.normal(0,0.01,nt), 0.05, 0.5)

    def arr(x):
        return x.reshape(nt,1,1)

    return xr.Dataset(
        {
            "tp": (("time","latitude","longitude"), arr(rain_mm/1000), {"units": "m"}),
            "t2m": (("time","latitude","longitude"), arr(t_c+273.15), {"units": "K"}),
            "relative_humidity_2m": (("time","latitude","longitude"), arr(rh), {"units": "%"}),
            "wind_speed_10m": (("time","latitude","longitude"), arr(wind), {"units": "m s-1"}),
            "ssrd": (("time","latitude","longitude"), arr(ssrd), {"units": "J m-2"}),
            "sp": (("time","latitude","longitude"), arr(sp), {"units": "Pa"}),
            "soil_moisture": (("time","latitude","longitude"), arr(sm), {"units": "m3 m-3"}),
        },
        coords={"time": time, "latitude": lat, "longitude": lon}
    )

hist_path = TMP / "historical_small.nc"
fcst_path = TMP / "forecast_small.nc"
make_ds(hist_time, 1).to_netcdf(hist_path)
make_ds(fcst_time, 2).to_netcdf(fcst_path)

out_dir = TMP / "outputs"
cmd = [
    sys.executable, "-m", "agroclimate_indices.run_pipeline",
    "--forecast", str(fcst_path),
    "--historical", str(hist_path),
    "--output", str(out_dir),
    "--scales", "1", "3"
]
env = os.environ.copy()
env["PYTHONPATH"] = str(ROOT)
subprocess.run(cmd, cwd=ROOT, env=env, check=True)

ds = xr.open_dataset(out_dir / "agroclimate_indices.nc", decode_timedelta=False)

required = [
    "rainfall_total","rainfall_anomaly","rainfall_anomaly_percent","tercile_class",
    "spi_1","spi_3","spei_1","spei_3","onset_doy","false_start_risk",
    "cessation_doy","length_of_growing_period","dry_spell_days","wet_spell_days",
    "heavy_rain_days_20mm","growing_degree_days","heat_stress_days","cold_stress_days",
    "diurnal_temperature_range_mean","reference_et_total","crop_et_total","water_balance_total",
    "soil_moisture_anomaly","water_stress_index","solar_radiation_anomaly","wind_risk_days",
    "thi_mean","thi_risk_class","livestock_heat_stress_duration_days","night_recovery_failure_days",
    "livestock_water_demand_index","pasture_rainfall_index_10d","forage_growth_proxy",
    "pasture_drought_index","livestock_cold_stress_days","wind_chill_risk_days",
    "mud_wetness_risk","vector_suitability_proxy","surface_water_stress_proxy","dust_dryness_risk",
    "agro_pastoral_drought_risk","feed_water_stress_index","crop_livestock_livelihood_stress_score",
    "crop_residue_outlook","planting_grazing_conflict_risk","seasonal_advisory_class",
    "input_timing_suitability","water_allocation_pressure","transhumance_mobility_stress",
    "integrated_resilience_opportunity"
]

missing = [v for v in required if v not in ds.data_vars]
if missing:
    raise RuntimeError(f"Missing variables: {missing}")

print("Smoke test passed.")
print(f"Output variables: {len(ds.data_vars)}")
print(f"Output NetCDF: {out_dir / 'agroclimate_indices.nc'}")
