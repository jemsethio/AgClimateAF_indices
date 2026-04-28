#!/usr/bin/env bash
set -euo pipefail

agroclimate-indices \
  --forecast /Volumes/T7/agroclimate_data/raw/forecast_africa.nc \
  --historical /Volumes/T7/agroclimate_data/raw/era5_historical_africa.nc \
  --output /Volumes/T7/agroclimate_data/indices_outputs \
  --scales 1 3 6
