This repository contains the source code enclosed to the research article titled "A novel cloud-oriented procedure based on SAR and Multispectral data to detect burnt areas". The following instructions will help you to download the code, setup your environment and execute the workflow.


## Code download

Clone the content of this reporitory in a local folder (e.g. using 'git clone')

## Environment setup

This project has been tested and run in a Linux Ubuntu environment.

### Step 1

- Create the folder 'data' inside your local folder.

- Inside the file '.env', set the value for LOCAL_DATA_PATH to point to the relative path of the folder 'data' you created (e.g. 'LOCAL_DATA_PATH=/data')

- Create the following folders inside the folder 'data':
    - 's1-pre'
    - 's1-post'
    - 's2-pre'
    - 's2-post'

- Copy into the folders created in the previous step the four input Sentinel images to be processed by the workflow in the form of the zipped products. For example:
    - 'S1A_IW_GRDH_1SDV_20210804T062638_20210804T062703_039076_049C51_D70E.zip' inside the folder 's1-pre'
    - 'S1A_IW_GRDH_1SDV_20210829T181128_20210829T181153_039448_04A911_FED5.zip' inside the folder 's1-post'
    - 'S2A_MSIL2A_20210809T110621_N0301_R137_T30TUK_20210809T143014.zip' inside  the folder 's2-pre'
    - 'S2A_MSIL2A_20210819T110621_N0301_R137_T30TUK_20210824T112618.zip' inside the folder  's2-post'


### Step 2

There are two alternative options to performi this step

##### Option 1 - automatic setup using Docker

- Install Docker Desktop

- Run the following command to create a Docker container, downloading and installing all the required dependences: 'sudo docker compose up -d --build --force-recreate'

##### Option 2 - manual setup

Install and configure the following tools
    Airflow
    ESAN SNAP (gpt)
    Python's package 'sentinelhub[AWS]'

### Step 3

- Copy the content of the folder  'dags' into the Airflow's dag folder (tipically "airflow/dags"

- Export all environment variables included the file '.env' into Airflow's execution context

## Workflow execution

- Open the following url in a web browser 'http://localhost:8080' (use the following credentials "airflow:airflow" or "admin:airflow")

- Run the 's1_s2_fire_detection' DAG, wait for completion and then access the produced results