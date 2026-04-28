import xarray as xr

def agro_pastoral_drought_risk(spei_score, rainfall_deficit, dry_spell_score, soil_moisture_deficit):
    out = 0.35 * spei_score + 0.25 * rainfall_deficit + 0.20 * dry_spell_score + 0.20 * soil_moisture_deficit
    out.name = "agro_pastoral_drought_risk"
    return out.clip(0, 1)

def feed_water_stress_index(pasture_drought, surface_water_stress, heat_stress):
    out = 0.40 * pasture_drought + 0.30 * surface_water_stress + 0.30 * heat_stress
    out.name = "feed_water_stress_index"
    return out.clip(0, 1)

def crop_livestock_livelihood_stress_score(crop_water_stress, pasture_stress, heat_stress):
    out = 0.40 * crop_water_stress + 0.35 * pasture_stress + 0.25 * heat_stress
    out.name = "crop_livestock_livelihood_stress_score"
    return out.clip(0, 1)

def crop_residue_outlook(rainfall_score, onset_score, lgp_score, gdd_score):
    out = 0.35 * rainfall_score + 0.20 * onset_score + 0.25 * lgp_score + 0.20 * gdd_score
    out.name = "crop_residue_outlook"
    return out.clip(0, 1)

def planting_grazing_conflict_risk(delayed_onset_score, pasture_shortage_score, crop_land_pressure_score):
    out = 0.40 * delayed_onset_score + 0.40 * pasture_shortage_score + 0.20 * crop_land_pressure_score
    out.name = "planting_grazing_conflict_risk"
    return out.clip(0, 1)

def seasonal_advisory_class(score):
    out = xr.where(score < 0.25, 0, xr.where(score < 0.50, 1, xr.where(score < 0.75, 2, 3))).astype("int16")
    out.name = "seasonal_advisory_class"
    out.attrs["classes"] = "0=Normal,1=Watch,2=Alert,3=Emergency"
    return out

def input_timing_suitability(onset_score, rainfall_probability_score, dry_spell_risk_score):
    out = 0.40 * onset_score + 0.35 * rainfall_probability_score + 0.25 * (1 - dry_spell_risk_score)
    out.name = "input_timing_suitability"
    return out.clip(0, 1)

def water_allocation_pressure(crop_water_demand_score, livestock_water_demand_score, rainfall_deficit_score):
    out = 0.40 * crop_water_demand_score + 0.30 * livestock_water_demand_score + 0.30 * rainfall_deficit_score
    out.name = "water_allocation_pressure"
    return out.clip(0, 1)

def transhumance_mobility_stress(pasture_stress, water_stress, heat_stress):
    out = 0.40 * pasture_stress + 0.35 * water_stress + 0.25 * heat_stress
    out.name = "transhumance_mobility_stress"
    return out.clip(0, 1)

def integrated_resilience_opportunity(good_rainfall_score, low_heat_score, adequate_lgp_score):
    out = 0.40 * good_rainfall_score + 0.30 * low_heat_score + 0.30 * adequate_lgp_score
    out.name = "integrated_resilience_opportunity"
    return out.clip(0, 1)
