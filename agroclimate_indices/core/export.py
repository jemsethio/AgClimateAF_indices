import json
from pathlib import Path
import pandas as pd

def write_outputs(ds, out_dir):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    nc = out_dir / "agroclimate_indices.nc"
    ds.to_netcdf(nc)

    try:
        zarr = out_dir / "agroclimate_indices.zarr"
        ds.to_zarr(zarr, mode="w")
    except Exception:
        zarr = None

    summary = {
        "netcdf": str(nc),
        "zarr": str(zarr) if zarr else None,
        "variables": list(ds.data_vars),
        "attributes": dict(ds.attrs),
    }
    with open(out_dir / "summary.json", "w") as f:
        json.dump(summary, f, indent=2, default=str)

    # API-ready JSONL. Good for FastAPI/database ingestion.
    df = ds.to_dataframe().reset_index()
    jsonl = out_dir / "api_ready_grid_records.jsonl"
    coord_cols = [c for c in ["time", "lat", "lon", "lead_time", "month"] if c in df.columns]
    with open(jsonl, "w") as f:
        for _, row in df.iterrows():
            record = {"coords": {}, "indices": {}}
            for c in coord_cols:
                v = row[c]
                record["coords"][c] = None if pd.isna(v) else str(v) if c == "time" else float(v)
            for c in df.columns:
                if c in coord_cols:
                    continue
                v = row[c]
                record["indices"][c] = None if pd.isna(v) else float(v)
            f.write(json.dumps(record) + "\n")

    return {"netcdf": nc, "jsonl": jsonl, "zarr": zarr}
