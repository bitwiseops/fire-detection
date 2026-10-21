# Sentinel-1 / Sentinel-2 burnt-area detection

Cloud-oriented workflow that detects burnt areas by fusing **SAR (Sentinel-1)** and **multispectral (Sentinel-2)** imagery, orchestrated with **Apache Airflow** and **ESA SNAP**, and packaged with Docker.

This repository contains the source code accompanying the research article *"Design and evaluation of a cloud-oriented procedure based on SAR and Multispectral data to detect burnt areas"*.

## How it works

The `s1_s2_fire_detection` DAG (see [`dags/`](dags/)) takes four pre-/post-fire Sentinel products plus the CORINE land-cover map and runs the whole chain unattended:

1. **Sentinel-1 (SAR)**: land-sea mask, orbit file, thermal-noise removal, calibration, speckle filtering and terrain correction on both dates, then pre/post **difference and ratio** images.
2. **Sentinel-2 (optical)**: resampling of both dates and computation of **dNBR**.
3. **Fusion**: the SAR and optical layers are collocated, merged, and cleaned of isolated noise pixels.

## Quick start

Requirements: Docker Desktop (the workflow was tested on Ubuntu).

```bash
git clone https://github.com/bitwiseops/fire-detection.git && cd fire-detection
cp .env.example .env          # set LOCAL_DATA_PATH if you use a different folder
```

The `data/` sub-folders already exist; copy one input product into each (zipped, as downloaded):

| Folder | Input |
|---|---|
| `data/s1-pre`, `data/s1-post` | Sentinel-1 GRDH scene before / after the fire (e.g. `S1A_IW_GRDH_1SDV_20210804T…zip`) |
| `data/s2-pre`, `data/s2-post` | Sentinel-2 L2A scene before / after the fire (e.g. `S2A_MSIL2A_20210809T…zip`) |
| `data/corine` | CORINE Land Cover raster (`U2018_CLC2018_V2020_20u1.tif`) |

Then start the stack and run the workflow:

```bash
docker compose up -d --build --force-recreate
```

Open <http://localhost:8080> (credentials `airflow` / `airflow`), trigger the **`s1_s2_fire_detection`** DAG and collect the produced rasters when it finishes.

## Stack

Python · Apache Airflow · ESA SNAP · Docker Compose · Sentinel-1 / Sentinel-2 · CORINE Land Cover

## License

MIT, see [LICENSE](LICENSE).
