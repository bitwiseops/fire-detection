# Airflow workflows

## Setup

### Common

- Copy `.env.sample` to `.env` and setup required values
- Create following folders into `LOCAL_DATA_PATH` specified in `.env`
    - `s1-pre`
    - `s1-post`
    - `s2-pre`
    - `s2-post`
- Place Sentinel-1 and Sentinel-2 zipped products  into the appropriate folder
    e.g 
    - `S1A_IW_GRDH_1SDV_20210804T062638_20210804T062703_039076_049C51_D70E.zip` -> `s1-pre`
    - `S1A_IW_GRDH_1SDV_20210829T181128_20210829T181153_039448_04A911_FED5.zip` -> `s1-post`
    - `S2A_MSIL2A_20210809T110621_N0301_R137_T30TUK_20210809T143014.zip` -> `s2-pre`
    - `S2A_MSIL2A_20210819T110621_N0301_R137_T30TUK_20210824T112618.zip` -> `s2-post`


### Docker

- Install Docker Desktop

### Local

- Install Airflow
- Install SNAP (gpt)
- Install Python's package `sentinelhub[AWS]`
- Setup environment vars listed in `.env.sample`

## Run

### Docker

- `sudo docker compose up -d --build --force-recreate`

### Local

- Copy `dags` content into Airflow's DAG loading folder
- Export all `.env` environment vars into Airflow's execution context
- Run Airflow

### Common

- Go to `http://localhost:8080` (airflow:airflow or admin:airflow)
- Run `s1_s2_fire_detection` DAG


