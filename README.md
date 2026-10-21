<div align="center">

# 🔥 Sentinel-1 / Sentinel-2 Burnt-Area Detection

**How do you map a burnt area when optical imagery alone is not enough?**
An Airflow and ESA SNAP workflow that fuses Sentinel-1 SAR with Sentinel-2 multispectral imagery.

![Python](https://img.shields.io/badge/python-3.6-blue)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)
![Sentinel-1](https://img.shields.io/badge/data-Sentinel--1%20GRD-1f6f8b)
![Sentinel-2](https://img.shields.io/badge/data-Sentinel--2%20L2A-1f6f8b)
![Airflow](https://img.shields.io/badge/orchestration-Apache%20Airflow-017cee)
![Docker](https://img.shields.io/badge/runtime-Docker-2496ed)

</div>

---

## Table of contents

- [Overview](#overview)
- [Highlights](#highlights)
- [Quick start](#quick-start)
- [How it works](#how-it-works)
- [Inputs](#inputs)
- [Project structure](#project-structure)
- [Limitations](#limitations)
- [Reference](#reference)
- [License](#license)

## Overview

This repository holds the source code accompanying the research article *"Design and evaluation of a cloud-oriented procedure based on SAR and Multispectral data to detect burnt areas"*. Optical data is blocked by smoke and cloud, SAR is not, but SAR alone is harder to interpret. The workflow processes both sources in one reproducible, containerised pipeline.

> **In short.** Give it one Sentinel-1 and one Sentinel-2 product from before the fire, the same pair from after, and a CORINE land-cover raster. Trigger one Airflow DAG and it produces the fused burnt-area layers with no manual steps.

## Highlights

- 📡 **SAR branch:** land-sea mask, orbit file, thermal-noise removal, calibration, speckle filter and terrain correction on both dates, then **difference** and **ratio** images.
- 🛰️ **Optical branch:** resampling of the pre/post Sentinel-2 scenes and **dNBR**.
- 🔀 **Fusion:** SAR and optical layers are collocated, merged and cleaned of isolated noise pixels.
- ☁️ **Cloud-oriented:** every step is an Airflow task, so the chain is visible, restartable and portable.
- 📦 **One command to run:** SNAP 8 and all dependencies are built inside the Docker image.

## Quick start

Requires Docker Desktop. The workflow was tested on Ubuntu.

```bash
git clone https://github.com/bitwiseops/fire-detection.git
cd fire-detection
cp .env.example .env          # set LOCAL_DATA_PATH if you use a different folder

# put the five input products in data/ (see "Inputs"), then:
docker compose up -d --build --force-recreate
```

Open <http://localhost:8080> (user `airflow`, password `airflow`), trigger the **`s1_s2_fire_detection`** DAG and collect the produced rasters when it finishes.

## How it works

1. **Sentinel-1:** land-sea mask → orbit file → thermal-noise removal → calibration → speckle filter → ellipsoid correction, on the pre and post scene.
2. **SAR change:** collocate the two dates, compute difference and ratio images and their means.
3. **Sentinel-2:** resample both dates, collocate, compute **dNBR**.
4. **Fusion:** collocate the SAR and optical products, merge, delete noise pixels.

The DAG is triggered manually (`schedule_interval=None`). The SNAP operators are wrapped in [`dags/operators/gpf.py`](dags/operators/gpf.py), and raster post-processing is in [`dags/operators/raster.py`](dags/operators/raster.py).

## Inputs

The `data/` sub-folders already exist. Put exactly **one zipped product** in each (the DAG stops if it finds more than one):

| Folder | Input |
|---|---|
| `data/s1-pre`, `data/s1-post` | Sentinel-1 GRDH scene before / after the fire, e.g. `S1A_IW_GRDH_1SDV_20210804T…zip` |
| `data/s2-pre`, `data/s2-post` | Sentinel-2 L2A scene before / after the fire, e.g. `S2A_MSIL2A_20210809T…zip` |
| `data/corine` | CORINE Land Cover raster, `U2018_CLC2018_V2020_20u1.tif` |

## Project structure

```
├── dags/
│   ├── s1_s2_fire_detection.py   # the Airflow DAG
│   └── operators/                # gpf.py (SNAP), net.py (download), raster.py (post-processing)
├── data/                         # input folders: s1-pre, s1-post, s2-pre, s2-post, corine
├── Dockerfile                    # Airflow image with ESA SNAP 8
├── docker-compose.yaml           # Airflow stack
└── .env.example                  # AIRFLOW_UID, LOCAL_DATA_PATH
```

## Limitations

The workflow processes one fire event (one pair of scenes) per run. The Docker image is based on an older Airflow with Python 3.6 and SNAP 8, and has been tested only on Ubuntu.

## Reference

*Design and evaluation of a cloud-oriented procedure based on SAR and Multispectral data to detect burnt areas.* The article reports the evaluation of the procedure.

## License

[MIT](LICENSE) © 2024 Flavio Cordari
