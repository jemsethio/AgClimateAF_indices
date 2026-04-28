import argparse
from pathlib import Path
import xarray as xr
import numpy as np

from agroclimate_indices.core.io import open_netcdf, read_yaml
from agroclimate_indices.core.prepare import harmonize_variables
from agroclimate_indices.core.temporal import aggregate_to_daily, monthly_sum
from agroclimate_indices.core.climatology import build_climatology
from agroclimate_indices.core.stats import minmax_score, safe_divide
from agroclimate_indices.core.export import write_outputs

from agroclimate_indices.crop.et0 import fao56_et0_daily
from agroclimate_indices.crop.rainfall import (
    rainfall_total, rainfall_anomaly, rainfall_anomaly_percent,
    tercile_class, dry_spell_days, max_consecutive_dry_spell,
    wet_spell_days, max_consecutive_wet_spell, extreme_rainfall_days
)
from agroclimate_indices.crop.drought import spi_gamma, spei_standardized
from agroclimate_indices.crop.seasonality import (
    onset_of_rainy_season, false_start_risk, cessation_date, length_of_growing_period
)
from agroclimate_indices.crop.temperature import (
    growing_degree_days, heat_stress_days, cold_stress_days, diurnal_temperature_range
)
from agroclimate_indices.crop.water_balance import crop_et, water_balance, water_stress_index
from agroclimate_indices.crop.radiation_wind import wind_risk_days

from agroclimate_indices.livestock.heat import (
    temperature_humidity_index, thi_risk_class, heat_stress_duration,
    max_heat_stress_spell, night_recovery_index, water_demand_index
)
from agroclimate_indices.livestock.pasture import (
    pasture_rainfall_index, temperature_suitability, forage_growth_proxy, pasture_drought_index
)
from agroclimate_indices.livestock.cold_wet_dust_vector import (
    livestock_cold_stress_index, wind_chill_index, wind_chill_risk,
    mud_wetness_risk, vector_suitability_proxy, surface_water_stress_proxy, dust_dryness_risk
)
from agroclimate_indices.integrated.scores import (
    agro_pastoral_drought_risk, feed_water_stress_index,
    crop_livestock_livelihood_stress_score, crop_residue_outlook,
    planting_grazing_conflict_risk, seasonal_advisory_class,
    input_timing_suitability, water_allocation_pressure,
    transhumance_mobility_stress, integrated_resilience_opportunity
)

PACKAGE_DIR = Path(__file__).resolve().parent

def add_et0(ds):
    required = [
        "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean",
        "relative_humidity_2m", "wind_speed_10m", "shortwave_radiation",
        "surface_pressure"
    ]
    if all(v in ds for v in required):
        ds["et0"] = fao56_et0_daily(
            ds["temperature_2m_max"],
            ds["temperature_2m_min"],
            ds["temperature_2m_mean"],
            ds["relative_humidity_2m"],
            ds["wind_speed_10m"],
            ds["shortwave_radiation"],
            ds["surface_pressure"],
        )
    return ds

def compute_all_indices(forecast_daily, historical_daily, clim, scales):
    th = read_yaml(PACKAGE_DIR / "config" / "thresholds.yaml")
    rain_th = th["rainfall"]
    crop_th = th["crop"]
    live_th = th["livestock"]

    out = xr.Dataset()

    p = forecast_daily["precipitation"]
    hp = historical_daily["precipitation"]

    # Forecast-period climatological total for the same day-of-year sequence.
    doy = p["time"].dt.dayofyear
    p_clim_daily = clim["precip_doy_mean"].sel(dayofyear=doy).drop_vars("dayofyear")
    p_clim_daily = p_clim_daily.assign_coords(time=p.time)
    p_total = rainfall_total(p)
    p_clim_total = p_clim_daily.sum("time")

    out["rainfall_total"] = p_total
    out["rainfall_anomaly"] = rainfall_anomaly(p_total, p_clim_total)
    out["rainfall_anomaly_percent"] = rainfall_anomaly_percent(p_total, p_clim_total)

    # Monthly tercile thresholds from historical monthly totals.
    hist_monthly_p = monthly_sum(hp)
    forecast_monthly_p = monthly_sum(p)
    if forecast_monthly_p.sizes.get("time", 0) > 0:
        # For period-level output, use sum over forecast horizon against historical monthly distribution.
        p33 = hist_monthly_p.quantile(0.33, dim="time")
        p66 = hist_monthly_p.quantile(0.66, dim="time")
        out["tercile_class"] = tercile_class(p_total, p33, p66)

    # SPI/SPEI at 1,3,6 months. Save last valid lead value for compact period output.
    for scale in scales:
        spi = spi_gamma(p, hp, scale_months=scale)
        if spi.sizes.get("time", 0) > 0:
            out[f"spi_{scale}"] = spi.isel(time=-1)

    # ET0, ETc, WB
    if "et0" in forecast_daily:
        et0 = forecast_daily["et0"]
        hist_et0 = historical_daily["et0"] if "et0" in historical_daily else None
        etc = crop_et(et0, kc=crop_th["kc_default"])
        wb = water_balance(p, etc)
        aet_proxy = xr.where(p < etc, p, etc)

        out["reference_et_total"] = et0.sum("time")
        out["crop_et_total"] = etc.sum("time")
        out["water_balance_total"] = wb.sum("time")
        out["water_stress_index"] = water_stress_index(aet_proxy.sum("time"), etc.sum("time"))

        if hist_et0 is not None:
            for scale in scales:
                spei = spei_standardized(p, et0, hp, hist_et0, scale_months=scale)
                if spei.sizes.get("time", 0) > 0:
                    out[f"spei_{scale}"] = spei.isel(time=-1)

    # Seasonality
    onset = onset_of_rainy_season(
        p,
        onset_total_mm=rain_th["onset_total_mm"],
        onset_window_days=rain_th["onset_window_days"],
        search_start_doy=rain_th["onset_search_start_doy"],
        search_end_doy=rain_th["onset_search_end_doy"],
        dry_day_mm=rain_th["dry_day_mm"],
        false_start_check_days=rain_th["false_start_check_days"],
        false_start_dry_spell_days=rain_th["false_start_dry_spell_days"],
    )
    out["onset_doy"] = onset
    out["false_start_risk"] = false_start_risk(
        p, onset,
        check_days=rain_th["false_start_check_days"],
        dry_day_mm=rain_th["dry_day_mm"],
        dry_spell_days=rain_th["false_start_dry_spell_days"],
    )
    if "et0" in forecast_daily:
        cess = cessation_date(
            p, forecast_daily["et0"],
            search_start_doy=rain_th["cessation_search_start_doy"],
            water_balance_threshold_mm=rain_th["cessation_water_balance_threshold_mm"],
        )
        out["cessation_doy"] = cess
        out["length_of_growing_period"] = length_of_growing_period(onset, cess)

    # Rainfall event indices
    out["dry_spell_days"] = dry_spell_days(p, rain_th["dry_day_mm"])
    out["max_consecutive_dry_spell"] = max_consecutive_dry_spell(p, rain_th["dry_day_mm"])
    out["wet_spell_days"] = wet_spell_days(p, rain_th["wet_day_mm"])
    out["max_consecutive_wet_spell"] = max_consecutive_wet_spell(p, rain_th["wet_day_mm"])
    out["heavy_rain_days_20mm"] = extreme_rainfall_days(p, rain_th["heavy_rain_mm"]).rename("heavy_rain_days_20mm")
    out["very_heavy_rain_days_50mm"] = extreme_rainfall_days(p, rain_th["very_heavy_rain_mm"]).rename("very_heavy_rain_days_50mm")
    p95 = clim["precip_doy_p95"].sel(dayofyear=doy).drop_vars("dayofyear").assign_coords(time=p.time)
    out["extreme_rain_days_p95"] = (p >= p95).sum("time")

    # Temperature crop indices
    if all(v in forecast_daily for v in ["temperature_2m_max", "temperature_2m_min"]):
        tmax = forecast_daily["temperature_2m_max"]
        tmin = forecast_daily["temperature_2m_min"]
        out["growing_degree_days"] = growing_degree_days(tmax, tmin, crop_th["tbase_c"])
        out["heat_stress_days"] = heat_stress_days(tmax, crop_th["heat_stress_tmax_c"])
        out["cold_stress_days"] = cold_stress_days(tmin, crop_th["cold_stress_tmin_c"])
        out["diurnal_temperature_range_mean"] = diurnal_temperature_range(tmax, tmin)

    if "soil_moisture" in forecast_daily and "soil_moisture_doy_mean" in clim:
        sm = forecast_daily["soil_moisture"]
        sm_clim = clim["soil_moisture_doy_mean"].sel(dayofyear=doy).drop_vars("dayofyear").assign_coords(time=p.time)
        out["soil_moisture_mean"] = sm.mean("time")
        out["soil_moisture_anomaly"] = sm.mean("time") - sm_clim.mean("time")

    if "shortwave_radiation" in forecast_daily and "shortwave_radiation_doy_mean" in clim:
        sr = forecast_daily["shortwave_radiation"]
        sr_clim = clim["shortwave_radiation_doy_mean"].sel(dayofyear=doy).drop_vars("dayofyear").assign_coords(time=p.time)
        out["solar_radiation_total"] = sr.sum("time")
        out["solar_radiation_anomaly"] = sr.sum("time") - sr_clim.sum("time")

    if "wind_speed_10m" in forecast_daily:
        out["wind_risk_days"] = wind_risk_days(forecast_daily["wind_speed_10m"], crop_th["wind_risk_ms"])

    # Livestock
    if all(v in forecast_daily for v in ["temperature_2m_mean", "relative_humidity_2m"]):
        thi = temperature_humidity_index(forecast_daily["temperature_2m_mean"], forecast_daily["relative_humidity_2m"])
        out["thi_mean"] = thi.mean("time")
        out["thi_max"] = thi.max("time")
        out["thi_risk_class"] = thi_risk_class(out["thi_max"])
        out["livestock_heat_stress_duration_days"] = heat_stress_duration(thi, live_th["thi_moderate"])
        out["max_livestock_heat_stress_spell"] = max_heat_stress_spell(thi, live_th["thi_moderate"])
        out["livestock_water_demand_index"] = water_demand_index(
            thi, live_th["baseline_water_litre_per_day"]
        ).mean("time")

    if "temperature_2m_min" in forecast_daily:
        out["night_recovery_failure_days"] = night_recovery_index(
            forecast_daily["temperature_2m_min"], live_th["night_recovery_tmin_c"]
        )
        out["livestock_cold_stress_days"] = livestock_cold_stress_index(
            forecast_daily["temperature_2m_min"], live_th["cold_stress_tmin_c"]
        )

    out["pasture_rainfall_index_10d"] = pasture_rainfall_index(p, hp, 10).isel(time=-1)
    out["pasture_rainfall_index_30d"] = pasture_rainfall_index(p, hp, 30).isel(time=-1)
    out["pasture_rainfall_index_90d"] = pasture_rainfall_index(p, hp, 90).isel(time=-1)

    rainfall_deficit = minmax_score(-out["rainfall_anomaly_percent"])
    dry_spell_score = minmax_score(out["max_consecutive_dry_spell"])
    et0_excess = minmax_score(out["reference_et_total"]) if "reference_et_total" in out else rainfall_deficit
    if "soil_moisture_anomaly" in out:
        sm_deficit = minmax_score(-out["soil_moisture_anomaly"])
        sm_score = minmax_score(out["soil_moisture_mean"])
    else:
        sm_deficit = rainfall_deficit
        sm_score = 1 - rainfall_deficit

    if "temperature_2m_mean" in forecast_daily:
        temp_suit = temperature_suitability(forecast_daily["temperature_2m_mean"]).mean("time")
    else:
        temp_suit = 1 - rainfall_deficit

    out["forage_growth_proxy"] = forage_growth_proxy(1 - rainfall_deficit, sm_score, temp_suit)
    out["pasture_drought_index"] = pasture_drought_index(rainfall_deficit, et0_excess, sm_deficit)

    wet_spell_score = minmax_score(out["max_consecutive_wet_spell"])
    rainfall_excess_score = minmax_score(out["rainfall_anomaly_percent"])
    out["mud_wetness_risk"] = mud_wetness_risk(wet_spell_score, rainfall_excess_score)

    out["surface_water_stress_proxy"] = surface_water_stress_proxy(rainfall_deficit, et0_excess, dry_spell_score)

    if all(v in forecast_daily for v in ["temperature_2m_mean", "wind_speed_10m"]):
        wci = wind_chill_index(forecast_daily["temperature_2m_mean"], forecast_daily["wind_speed_10m"])
        out["wind_chill_index_mean"] = wci.mean("time")
        out["wind_chill_risk_days"] = wind_chill_risk(wci)

    if all(v in forecast_daily for v in ["relative_humidity_2m", "wind_speed_10m"]):
        wind_score = minmax_score(forecast_daily["wind_speed_10m"].mean("time"))
        low_humidity_score = minmax_score(-forecast_daily["relative_humidity_2m"].mean("time"))
        out["dust_dryness_risk"] = dust_dryness_risk(dry_spell_score, wind_score, low_humidity_score)

    if all(v in forecast_daily for v in ["temperature_2m_mean", "relative_humidity_2m"]):
        rain_suit = (out["rainfall_total"] > 50).astype(float)
        humid_suit = (forecast_daily["relative_humidity_2m"].mean("time") > 60).astype(float)
        temp_suit_binary = ((forecast_daily["temperature_2m_mean"].mean("time") >= 18) &
                            (forecast_daily["temperature_2m_mean"].mean("time") <= 32)).astype(float)
        out["vector_suitability_proxy"] = vector_suitability_proxy(rain_suit, humid_suit, temp_suit_binary)

    # Integrated scores
    if any(f"spei_{s}" in out for s in scales):
        spei_name = f"spei_{max([s for s in scales if f'spei_{s}' in out])}"
        spei_score = minmax_score(-out[spei_name])
    else:
        spei_score = rainfall_deficit

    heat_stress = minmax_score(out["thi_max"]) if "thi_max" in out else minmax_score(out["heat_stress_days"]) if "heat_stress_days" in out else rainfall_deficit
    crop_water_stress = out["water_stress_index"] if "water_stress_index" in out else rainfall_deficit

    out["agro_pastoral_drought_risk"] = agro_pastoral_drought_risk(spei_score, rainfall_deficit, dry_spell_score, sm_deficit)
    out["feed_water_stress_index"] = feed_water_stress_index(out["pasture_drought_index"], out["surface_water_stress_proxy"], heat_stress)
    out["crop_livestock_livelihood_stress_score"] = crop_livestock_livelihood_stress_score(crop_water_stress, out["pasture_drought_index"], heat_stress)

    rainfall_score = minmax_score(out["rainfall_total"])
    onset_score = minmax_score(-out["onset_doy"])
    lgp_score = minmax_score(out["length_of_growing_period"]) if "length_of_growing_period" in out else rainfall_score
    gdd_score = minmax_score(out["growing_degree_days"]) if "growing_degree_days" in out else rainfall_score
    out["crop_residue_outlook"] = crop_residue_outlook(rainfall_score, onset_score, lgp_score, gdd_score)

    delayed_onset_score = minmax_score(out["onset_doy"])
    out["planting_grazing_conflict_risk"] = planting_grazing_conflict_risk(delayed_onset_score, out["pasture_drought_index"], 1 - out["crop_residue_outlook"])
    out["input_timing_suitability"] = input_timing_suitability(onset_score, rainfall_score, out["false_start_risk"])
    out["water_allocation_pressure"] = water_allocation_pressure(
        minmax_score(out["crop_et_total"]) if "crop_et_total" in out else rainfall_deficit,
        minmax_score(out["livestock_water_demand_index"]) if "livestock_water_demand_index" in out else heat_stress,
        rainfall_deficit
    )
    out["transhumance_mobility_stress"] = transhumance_mobility_stress(out["pasture_drought_index"], out["surface_water_stress_proxy"], heat_stress)
    out["integrated_resilience_opportunity"] = integrated_resilience_opportunity(rainfall_score, 1 - heat_stress, lgp_score)

    out["seasonal_advisory_score"] = out["crop_livestock_livelihood_stress_score"]
    out["seasonal_advisory_class"] = seasonal_advisory_class(out["seasonal_advisory_score"])

    out.attrs["note"] = "All climatologies derived internally from supplied historical NetCDF baseline."
    return out

def run(args):
    variable_map = read_yaml(PACKAGE_DIR / "config" / "variable_map.yaml")
    thresholds = read_yaml(PACKAGE_DIR / "config" / "thresholds.yaml")

    forecast_raw = open_netcdf(args.forecast, chunks={"time": 256})
    historical_raw = open_netcdf(args.historical, chunks={"time": 512})

    forecast_std = harmonize_variables(forecast_raw, variable_map)
    historical_std = harmonize_variables(historical_raw, variable_map)

    forecast_daily = aggregate_to_daily(forecast_std)
    historical_daily = aggregate_to_daily(historical_std)

    forecast_daily = add_et0(forecast_daily)
    historical_daily = add_et0(historical_daily)

    clim = build_climatology(
        historical_daily,
        thresholds["baseline"]["start"],
        thresholds["baseline"]["end"],
    )

    out = compute_all_indices(
        forecast_daily=forecast_daily,
        historical_daily=historical_daily,
        clim=clim,
        scales=args.scales,
    )

    outputs = write_outputs(out, args.output)
    print("DONE")
    for k, v in outputs.items():
        print(f"{k}: {v}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--forecast", required=True, help="Raw hourly/3-hourly/6-hourly forecast NetCDF")
    parser.add_argument("--historical", required=True, help="Raw hourly/3-hourly/6-hourly historical NetCDF, e.g. ERA5/ERA5-Land")
    parser.add_argument("--output", required=True, help="Output directory")
    parser.add_argument("--scales", nargs="+", type=int, default=[1, 3, 6], help="SPI/SPEI scales in months")
    args = parser.parse_args()
    run(args)

if __name__ == "__main__":
    main()
