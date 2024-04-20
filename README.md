# Airflow workflows

## Setup

### Common

- Copy `.env.sample` to `.env` and setup required values

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


