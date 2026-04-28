# AgroClimate Indices Package

The AgroClimate Indices Engine is Python pipeline that transforms raw climate data (hourly to seasonal forecast) into standardized agro-climate indicators for crop, livestock, and integrated farming systems. It automatically derives both multimodel forecast & climatology from historical datasets and computes a comprehensive suite of indices—including rainfall anomalies, SPI/SPEI, onset and growing period metrics, evapotranspiration, heat stress, pasture conditions, and integrated livelihood risk scores.

The pippline is designed for operational climate services, enabling seamless integration into platforms such as EDACaP and the AgroClimate Africa API, and supporting early warning, advisory generation, and climate risk decision-making at scale. This package calculates crop-focused, livestock-focused, and integrated crop-livestock agro-climate indices from raw NetCDF forecast and historical data at different time scale inluding, hourly/3-hourly/6-hourly NetCDF data, which depend on the initiliztion of the model

## ⚙️ Key Features
✅ Works directly with hourly / 3-hourly / 6-hourly NetCDF

✅ Automatically builds climatology from historical data

✅ Supports ERA5, ERA5-Land, CHIRPS, SEAS5, NMME, ECMWF_AIF, WeatherNEXT,..

✅ Computes complete agro-climate index suite

✅ Generates API-ready outputs

✅ Scalable to continental (Africa-wide) deployment

✅ Designed for operational advisory systems

## Agro-Climate Indices

All indices are derived dynamically from bias-corrected forecast climate data.

### Crop Indices

| Index                   | Equation / Logic         | Unit    | Interpretation           |
| ----------------------- | ------------------------ | ------- | ------------------------ |
| Rainfall Total          | ΣP                       | mm      | Total water availability |
| Rainfall Anomaly        | Pforecast − Pclim        | mm      | Wet/dry deviation        |
| Rainfall Anomaly %      | % difference             | %       | Advisory thresholds      |
| Tercile Class           | Percentiles (33/66)      | BN/N/AN | Seasonal outlook         |
| SPI                     | Gamma → Normal transform | -       | Drought/wetness          |
| SPEI                    | Standardized (P − ET₀)   | -       | Heat-driven drought      |
| Onset                   | Rainfall threshold logic | DOY     | Planting start           |
| False Start             | Onset + dry spell        | binary  | Planting risk            |
| Cessation               | P − ET₀ threshold        | DOY     | End of season            |
| LGP                     | cessation − onset        | days    | Crop suitability         |
| Dry/Wet Spells          | Consecutive thresholds   | days    | Stress/disease           |
| Extreme Rain            | P ≥ threshold            | days    | Flood risk               |
| GDD                     | Σ(Tmean − Tbase)         | °C·days | Crop growth              |
| Heat Stress             | Tmax ≥ threshold         | days    | Yield loss               |
| Cold Stress             | Tmin ≤ threshold         | days    | Frost risk               |
| ET₀                     | FAO-56 Penman–Monteith   | mm/day  | Water demand             |
| ETc                     | Kc × ET₀                 | mm/day  | Crop water need          |
| Water Balance           | P − ETc                  | mm      | Deficit/surplus          |
| WSI                     | 1 − AET/ETc              | 0–1     | Water stress             |
| Soil Moisture Anomaly   | SM − SMclim              | -       | Drought signal           |
| Solar Radiation Anomaly | SRAD − SRADclim          | W/m²    | Growth signal            |
| Wind Risk               | Wind ≥ threshold         | days    | Lodging risk             |

### Livestock Indices
| Index                | Equation / Logic           | Unit     | Interpretation      |
| -------------------- | -------------------------- | -------- | ------------------- |
| THI                  | 0.8T + RH(T−14.4) + 46.4   | index    | Heat stress         |
| THI Class            | Threshold categories       | class    | Stress level        |
| Heat Duration        | Consecutive THI exceedance | days     | Persistence         |
| Night Recovery       | High Tmin/THI              | days     | Poor recovery       |
| Water Demand         | Baseline × f(THI)          | relative | Drinking need       |
| Pasture Rainfall     | Rolling anomaly            | %        | Forage availability |
| Forage Growth        | f(P, SM, T)                | 0–1      | Grazing potential   |
| Pasture Drought      | Combined stress            | 0–1      | Feed scarcity       |
| Cold Stress          | Tmin threshold             | days     | Cold exposure       |
| Wind Chill           | f(T, wind)                 | index    | Exposure risk       |
| Mud/Wetness          | Wet spells + rain          | days     | Mobility risk       |
| Vector Proxy         | T + RH + rainfall          | 0–1      | Disease risk        |
| Surface Water Stress | P deficit + ET₀            | 0–1      | Water pressure      |
| Dust Risk            | Dry + wind                 | days     | Respiratory risk    |

### Livestock Indices

| Index                | Equation / Logic           | Unit     | Interpretation      |
| -------------------- | -------------------------- | -------- | ------------------- |
| THI                  | 0.8T + RH(T−14.4) + 46.4   | index    | Heat stress         |
| THI Class            | Threshold categories       | class    | Stress level        |
| Heat Duration        | Consecutive THI exceedance | days     | Persistence         |
| Night Recovery       | High Tmin/THI              | days     | Poor recovery       |
| Water Demand         | Baseline × f(THI)          | relative | Drinking need       |
| Pasture Rainfall     | Rolling anomaly            | %        | Forage availability |
| Forage Growth        | f(P, SM, T)                | 0–1      | Grazing potential   |
| Pasture Drought      | Combined stress            | 0–1      | Feed scarcity       |
| Cold Stress          | Tmin threshold             | days     | Cold exposure       |
| Wind Chill           | f(T, wind)                 | index    | Exposure risk       |
| Mud/Wetness          | Wet spells + rain          | days     | Mobility risk       |
| Vector Proxy         | T + RH + rainfall          | 0–1      | Disease risk        |
| Surface Water Stress | P deficit + ET₀            | 0–1      | Water pressure      |
| Dust Risk            | Dry + wind                 | days     | Respiratory risk    |

### Integrated Indices

| Index                     | Logic                  | Interpretation      |
| ------------------------- | ---------------------- | ------------------- |
| Agro-Pastoral Drought     | SPEI + rainfall + SM   | Shared drought      |
| Feed–Water Stress         | Pasture + water + heat | Livestock pressure  |
| Livelihood Stress (CLSS)  | Crop + pasture + heat  | System risk         |
| Crop Residue Outlook      | Rainfall + LGP + GDD   | Feed availability   |
| Planting–Grazing Conflict | Onset + pasture        | Resource conflict   |
| Advisory Class            | Score → category       | Decision-ready      |
| Input Timing              | Onset + rainfall       | Planting/fertilizer |
| Water Allocation          | Demand vs rainfall     | Planning            |
| Mobility Stress           | Pasture + water        | Transhumance risk   |
| Resilience Opportunity    | Favorable conditions   | Opportunity         |

### Outputs
| File                           | Description     |
| ------------------------------ | --------------- |
| `agroclimate_indices.nc`       | Full dataset    |
| `agroclimate_indices.zarr`     | Cloud-optimized |
| `api_ready_grid_records.jsonl` | API ingestion   |
| `summary.json`                 | Metadata        |


## Install

```bash
uv venv .venv
source .venv/bin/activate
uv pip install -e .
```

## Run

```bash
agroclimate-indices \
  --forecast /Volumes/T7/agroclimate_data/raw/forecast_africa.nc \
  --historical /Volumes/T7/agroclimate_data/raw/era5_historical_africa.nc \
  --output /Volumes/T7/agroclimate_data/indices_outputs \
  --scales 1 3 6
```

## Outputs

```text
agroclimate_indices.nc
agroclimate_indices.zarr
summary.json
api_ready_grid_records.jsonl
```

## Important

Historical data should cover the selected baseline, preferably 1991-2020.
Forecast and historical grids should ideally be on the same grid. If not, regrid before running.


## Smoke test

A synthetic NetCDF test is included. It creates 6-hourly historical and forecast NetCDF files,
runs the full pipeline, and checks that all listed indices are present.

```bash
python examples/smoke_test_synthetic_netcdf.py
```

The pipeline was smoke-tested with synthetic 6-hourly NetCDF data containing:
`tp`, `t2m`, `relative_humidity_2m`, `wind_speed_10m`, `ssrd`, `sp`, and `soil_moisture`.
It successfully produced NetCDF and API-ready JSONL outputs.


#👤 Author

Jemal S. Ahmed
Alliance of Bioversity International & CIAT (CGIAR Climate Action)
📧 J.Ahmed@cgiar.org
