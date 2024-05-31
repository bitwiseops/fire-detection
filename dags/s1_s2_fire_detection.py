from datetime import datetime
import os
import glob
from airflow import DAG
from airflow.operators.python import PythonOperator

# from operators.net import download_aws
from operators.gpf import create_operator
from operators.raster import delete_noise

def get_zip_filename(folder_path):
    # Use glob to find all .zip files in the specified folder
    zip_files = glob.glob(os.path.join(folder_path, "*.zip"))
    
    # Check if there is exactly one .zip file
    if len(zip_files) == 1:
        return os.path.basename(zip_files[0])
    else:
        raise ValueError(f"Expected exactly one .zip file in {folder_path}, but found {len(zip_files)}")

def get_tif_filename(folder_path):
    # Use glob to find all .tif files in the specified folder
    tif_files = glob.glob(os.path.join(folder_path, "*.tif"))
    
    # Check if there is exactly one .tif file
    if len(tif_files) == 1:
        return os.path.basename(tif_files[0])
    else:
        raise ValueError(f"Expected exactly one .tif file in {folder_path}, but found {len(tif_files)}")


# ---GENERAL SETTINGS---
SUBPROCESS=False
GRAPH_ID = 's1_s2_fire_detection'
BASE_OUT_FOLDER = '/data'
PRODUCTS = {
    's1-pre': {
        'file': get_zip_filename(os.path.join(BASE_OUT_FOLDER, 's1-pre')),
    },
    's1-post': {
        'file': get_zip_filename(os.path.join(BASE_OUT_FOLDER, 's1-post')),
    },
    's2-pre': {
        'file': get_zip_filename(os.path.join(BASE_OUT_FOLDER, 's2-pre')),
    },
    's2-post': {
        'file': get_zip_filename(os.path.join(BASE_OUT_FOLDER, 's2-post')),
    },
}
CORINE = {
    'file': get_tif_filename(os.path.join(BASE_OUT_FOLDER, 'corine')),
}


# ---SCHEDULER SECTION---
##### S1 #####
Download0 = 'Download0'
Download0_OUT = 's1-pre'
Download0_QUEUE = 'default'
Download0_PRIORITY = 0

LandSeaMask0 = 'LandSeaMask0'
LandSeaMask0_OUT = LandSeaMask0 + '.dim'
LandSeaMask0_QUEUE = 'default'
LandSeaMask0_PRIORITY = 0

ApplyOrbitFile0 = 'ApplyOrbitFile0'
ApplyOrbitFile0_OUT = ApplyOrbitFile0 + '.dim'
ApplyOrbitFile0_QUEUE = 'default'
ApplyOrbitFile0_PRIORITY = 0

ThermalNoiseRemoval0 = 'ThermalNoiseRemoval0'
ThermalNoiseRemoval0_OUT = ThermalNoiseRemoval0 + '.dim'
ThermalNoiseRemoval0_QUEUE = 'default'
ThermalNoiseRemoval0_PRIORITY = 0

RemoveBorderNoise0 = 'RemoveBorderNoise0'
RemoveBorderNoise0_OUT = RemoveBorderNoise0 + '.dim'
RemoveBorderNoise0_QUEUE = 'default'
RemoveBorderNoise0_PRIORITY = 0

CalibrationSigma0 = 'CalibrationSigma0'
CalibrationSigma0_OUT = CalibrationSigma0 + '.dim'
CalibrationSigma0_QUEUE = 'default'
CalibrationSigma0_PRIORITY = 0

SpeckleFilter0 = 'SpeckleFilter0'
SpeckleFilter0_OUT = SpeckleFilter0 + '.dim'
SpeckleFilter0_QUEUE = 'default'
SpeckleFilter0_PRIORITY = 0

Ellipsoid0 = 'Ellipsoid0'
Ellipsoid0_OUT = Ellipsoid0 + '.dim'
Ellipsoid0_QUEUE = 'default'
Ellipsoid0_PRIORITY = 0

Download1 = 'Download1'
Download1_OUT = 's1-post'
Download1_QUEUE = 'default'
Download1_PRIORITY = 0

LandSeaMask1 = 'LandSeaMask1'
LandSeaMask1_OUT = LandSeaMask1 + '.dim'
LandSeaMask1_QUEUE = 'default'
LandSeaMask1_PRIORITY = 0

ApplyOrbitFile1 = 'ApplyOrbitFile1'
ApplyOrbitFile1_OUT = ApplyOrbitFile1 + '.dim'
ApplyOrbitFile1_QUEUE = 'default'
ApplyOrbitFile1_PRIORITY = 0

ThermalNoiseRemoval1 = 'ThermalNoiseRemoval1'
ThermalNoiseRemoval1_OUT = ThermalNoiseRemoval1 + '.dim'
ThermalNoiseRemoval1_QUEUE = 'default'
ThermalNoiseRemoval1_PRIORITY = 0

RemoveBorderNoise1 = 'RemoveBorderNoise1'
RemoveBorderNoise1_OUT = RemoveBorderNoise1 + '.dim'
RemoveBorderNoise1_QUEUE = 'default'
RemoveBorderNoise1_PRIORITY = 0

CalibrationSigma1 = 'CalibrationSigma1'
CalibrationSigma1_OUT = CalibrationSigma1 + '.dim'
CalibrationSigma1_QUEUE = 'default'
CalibrationSigma1_PRIORITY = 0

SpeckleFilter1 = 'SpeckleFilter1'
SpeckleFilter1_OUT = SpeckleFilter1 + '.dim'
SpeckleFilter1_QUEUE = 'default'
SpeckleFilter1_PRIORITY = 0

Ellipsoid1 = 'Ellipsoid1'
Ellipsoid1_OUT = Ellipsoid1 + '.dim'
Ellipsoid1_QUEUE = 'default'
Ellipsoid1_PRIORITY = 0

Collocate0 = "Collocate0"
Collocate0_OUT = Collocate0 + '.dim'
Collocate0_QUEUE = 'default'
Collocate0_PRIORITY = 0

Diff0 = "Diff0"
Diff0_OUT = Diff0 + '.dim'
Diff0_QUEUE = 'default'
Diff0_PRIORITY = 0

Ratio0 = "Ratio0"
Ratio0_OUT = Ratio0 + '.dim'
Ratio0_QUEUE = 'default'
Ratio0_PRIORITY = 0

DiffMean0 = "DiffMean0"
DiffMean0_OUT = DiffMean0 + '.dim'
DiffMean0_QUEUE = 'default'
DiffMean0_PRIORITY = 0

RatioMean0 = "RatioMean0"
RatioMean0_OUT = RatioMean0 + '.dim'
RatioMean0_QUEUE = 'default'
RatioMean0_PRIORITY = 0

Collocate1 = "Collocate1"
Collocate1_OUT = Collocate1 + '.dim'
Collocate1_QUEUE = 'default'
Collocate1_PRIORITY = 0

##### S2 #####

Download2 = "Download2"
Download2_OUT = 's2-pre'
Download2_QUEUE = 'default'
Download2_PRIORITY = 0

Download3 = "Download3"
Download3_OUT = 's2-post'
Download3_QUEUE = 'default'
Download3_PRIORITY = 0

Resample0 = "Resample0"
Resample0_OUT = Resample0 + '.dim'
Resample0_QUEUE = 'default'
Resample0_PRIORITY = 0

Resample1 = "Resample1"
Resample1_OUT = Resample1 + '.dim'
Resample1_QUEUE = 'default'
Resample1_PRIORITY = 0

Collocate2 = "Collocate2"
Collocate2_OUT = Collocate2 + '.dim'
Collocate2_QUEUE = 'default'
Collocate2_PRIORITY = 0

dNBR0 = "dNBR0"
dNBR0_OUT = dNBR0 + '.dim'
dNBR0_QUEUE = 'default'
dNBR0_PRIORITY = 0

##### CORINE #####

Download4 = "Download4"
Download4_OUT = 'corine'
Download4_QUEUE = 'default'
Download4_PRIORITY = 0

Collocate3 = "Collocate3"
Collocate3_OUT = Collocate3 + '.dim'
Collocate3_QUEUE = 'default'
Collocate3_PRIORITY = 0

##### FIRE DETECT #####

Collocate4 = "Collocate4"
Collocate4_OUT = Collocate4 + '.dim'
Collocate4_QUEUE = 'default'
Collocate4_PRIORITY = 0

Merge0 = "Merge0"
Merge0_OUT = Merge0 + '.dim'
Merge0_QUEUE = 'default'
Merge0_PRIORITY = 0

DeleteNoise0 = "DeleteNoise0"
DeleteNoise0_OUT = DeleteNoise0 + '.geojson'
DeleteNoise0_QUEUE = 'default'
DeleteNoise0_PRIORITY = 0

with DAG(
    GRAPH_ID,
    default_args={
        'retries': 1,
    },
    description='Sentinel Fire Detection',
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['sentinel-1', 'sentinel-2', 'workflow'],
) as dag:


    land_sea_mask_op0 = create_operator(
        'Land-Sea-Mask',
        LandSeaMask0,
        BASE_OUT_FOLDER + "/" + Download0_OUT + '/' + PRODUCTS[Download0_OUT]['file'],
        BASE_OUT_FOLDER + "/" + LandSeaMask0_OUT,
        LandSeaMask0_QUEUE,
        LandSeaMask0_PRIORITY,
        parameters={
            'landMask': 'false',
            'useSRTM': 'true'
        },
    )
    
    apply_orbit_file_op0 = create_operator(
        'Apply-Orbit-File',
        ApplyOrbitFile0,
        BASE_OUT_FOLDER + "/" + LandSeaMask0_OUT,
        BASE_OUT_FOLDER + "/" + ApplyOrbitFile0_OUT,
        ApplyOrbitFile0_QUEUE,
        ApplyOrbitFile0_PRIORITY,
        parameters={
            'orbitType': 'Sentinel Precise (Auto Download)',
            'polyDegree': '3',
            'continueOnFail': 'true'
        },
    )

    thermal_noise_removal_op0 = create_operator(
        'ThermalNoiseRemoval',
        ThermalNoiseRemoval0,
        BASE_OUT_FOLDER + "/" + ApplyOrbitFile0_OUT,
        BASE_OUT_FOLDER + "/" + ThermalNoiseRemoval0_OUT,
        ThermalNoiseRemoval0_QUEUE,
        ThermalNoiseRemoval0_PRIORITY,
        parameters={
            'removeThermalNoise': 'true',
            'reIntroduceThermalNoise': 'false'
        }
    )

    calibration_op0 = create_operator(
        'Calibration',
        CalibrationSigma0,
        BASE_OUT_FOLDER + "/" + ThermalNoiseRemoval0_OUT,
        BASE_OUT_FOLDER + "/" + CalibrationSigma0_OUT,
        CalibrationSigma0_QUEUE,
        CalibrationSigma0_PRIORITY,
        parameters={
            'auxFile': 'Product Auxiliary File',
            'outputImageInComplex': 'false',
            'outputImageScaleInDb': 'false',
            'createGammaBand': 'false',
            'createBetaBand': 'false',
            'outputSigmaBand': 'true',
            'outputGammaBand': 'false',
            'outputBetaBand': 'false'
        }
    )

    speckle_filter_op0 = create_operator(
        'Speckle-Filter',
        SpeckleFilter0,
        BASE_OUT_FOLDER + "/" + CalibrationSigma0_OUT,
        BASE_OUT_FOLDER + "/" + SpeckleFilter0_OUT,
        SpeckleFilter0_QUEUE,
        SpeckleFilter0_PRIORITY,
        parameters={
            'filter': 'Lee Sigma',
            'filterSizeX': '3',
            'filterSizeY': '3',
            'dampingFactor': '2',
            'estimateENL': 'true',
            'enl': '1.0',
            'numLooksStr': '1',
            'windowSize': '7x7',
            'targetWindowSizeStr': '3x3',
            'sigmaStr': '0.9',
            'anSize': '50'
        }
    )

    ellipsoid_correction_op0 = create_operator(
        'Ellipsoid-Correction-GG',
        Ellipsoid0,
        BASE_OUT_FOLDER + "/" + SpeckleFilter0_OUT,
        BASE_OUT_FOLDER + "/" + Ellipsoid0_OUT,
        Ellipsoid0_QUEUE,
        Ellipsoid0_PRIORITY,
        parameters={
            'imgResamplingMethod': 'BILINEAR_INTERPOLATION', 
            'mapProjection': '''GEOGCS["WGS84(DD)", 
DATUM["WGS84", 
    SPHEROID["WGS84", 6378137.0, 298.257223563]], 
PRIMEM["Greenwich", 0.0], 
UNIT["degree", 0.017453292519943295], 
AXIS["Geodetic longitude", EAST], 
AXIS["Geodetic latitude", NORTH]]'''
        }
    )

    land_sea_mask_op0 >> apply_orbit_file_op0 >> thermal_noise_removal_op0 >> calibration_op0 >> speckle_filter_op0 >> ellipsoid_correction_op0


    land_sea_mask_op1 = create_operator(
        'Land-Sea-Mask',
        LandSeaMask1,
        BASE_OUT_FOLDER + "/" + Download1_OUT + '/' + PRODUCTS[Download1_OUT]['file'],
        BASE_OUT_FOLDER + "/" + LandSeaMask1_OUT,
        LandSeaMask1_QUEUE,
        LandSeaMask1_PRIORITY,
        parameters={
            'landMask': 'false',
            'useSRTM': 'true'
        },
    )
    
    apply_orbit_file_op1 = create_operator(
        'Apply-Orbit-File',
        ApplyOrbitFile1,
        BASE_OUT_FOLDER + "/" + LandSeaMask1_OUT,
        BASE_OUT_FOLDER + "/" + ApplyOrbitFile1_OUT,
        ApplyOrbitFile1_QUEUE,
        ApplyOrbitFile1_PRIORITY,
        parameters={
            'orbitType': 'Sentinel Precise (Auto Download)',
            'polyDegree': '3',
            'continueOnFail': 'true'
        },
    )

    thermal_noise_removal_op1 = create_operator(
        'ThermalNoiseRemoval',
        ThermalNoiseRemoval1,
        BASE_OUT_FOLDER + "/" + ApplyOrbitFile1_OUT,
        BASE_OUT_FOLDER + "/" + ThermalNoiseRemoval1_OUT,
        ThermalNoiseRemoval1_QUEUE,
        ThermalNoiseRemoval1_PRIORITY,
        parameters={
            'removeThermalNoise': 'true',
            'reIntroduceThermalNoise': 'false'
        }
    )

    calibration_op1 = create_operator(
        'Calibration',
        CalibrationSigma1,
        # BASE_OUT_FOLDER + "/" + RemoveBorderNoise1_OUT,
        BASE_OUT_FOLDER + "/" + ThermalNoiseRemoval1_OUT,
        BASE_OUT_FOLDER + "/" + CalibrationSigma1_OUT,
        CalibrationSigma1_QUEUE,
        CalibrationSigma1_PRIORITY,
        parameters={
            'auxFile': 'Product Auxiliary File',
            'outputImageInComplex': 'false',
            'outputImageScaleInDb': 'false',
            'createGammaBand': 'false',
            'createBetaBand': 'false',
            'outputSigmaBand': 'true',
            'outputGammaBand': 'false',
            'outputBetaBand': 'false'
        }
    )

    speckle_filter_op1 = create_operator(
        'Speckle-Filter',
        SpeckleFilter1,
        BASE_OUT_FOLDER + "/" + CalibrationSigma1_OUT,
        BASE_OUT_FOLDER + "/" + SpeckleFilter1_OUT,
        SpeckleFilter1_QUEUE,
        SpeckleFilter1_PRIORITY,
        parameters={
            'filter': 'Lee Sigma',
            'filterSizeX': '3',
            'filterSizeY': '3',
            'dampingFactor': '2',
            'estimateENL': 'true',
            'enl': '1.0',
            'numLooksStr': '1',
            'windowSize': '7x7',
            'targetWindowSizeStr': '3x3',
            'sigmaStr': '0.9',
            'anSize': '50'
        }
    )

    ellipsoid_correction_op1 = create_operator(
        'Ellipsoid-Correction-GG',
        Ellipsoid1,
        BASE_OUT_FOLDER + "/" + SpeckleFilter1_OUT,
        BASE_OUT_FOLDER + "/" + Ellipsoid1_OUT,
        Ellipsoid1_QUEUE,
        Ellipsoid1_PRIORITY,
        parameters={
            'imgResamplingMethod': 'BILINEAR_INTERPOLATION', 
            'mapProjection': '''GEOGCS["WGS84(DD)", 
DATUM["WGS84", 
    SPHEROID["WGS84", 6378137.0, 298.257223563]], 
PRIMEM["Greenwich", 0.0], 
UNIT["degree", 0.017453292519943295], 
AXIS["Geodetic longitude", EAST], 
AXIS["Geodetic latitude", NORTH]]'''
        }
    )

    land_sea_mask_op1 >> apply_orbit_file_op1 >> thermal_noise_removal_op1 >> calibration_op1 >> speckle_filter_op1 >> ellipsoid_correction_op1


    collocate_op0 = create_operator(
        'Collocate',
        Collocate0,
        BASE_OUT_FOLDER + "/" + Ellipsoid0_OUT,
        BASE_OUT_FOLDER + "/" + Collocate0_OUT,
        Collocate0_QUEUE,
        Collocate0_PRIORITY,
        parameters={
            "masterProductName": Ellipsoid0_OUT[:-4],
            "renameMasterComponents": "true",
            "renameSlaveComponents": "true",
            "masterComponentPattern": "${ORIGINAL_NAME}_M",
            "slaveComponentPattern": "${ORIGINAL_NAME}_S",
            "resamplingType": "NEAREST_NEIGHBOUR",
            "sourceProductPaths": ",".join([
            BASE_OUT_FOLDER + "/" + Ellipsoid1_OUT,
        ])
        }
    )

    ratio_op0 = create_operator(
        'BandMaths',
        Ratio0,
        BASE_OUT_FOLDER + "/" + Collocate0_OUT,
        BASE_OUT_FOLDER + "/" + Ratio0_OUT,
        Ratio0_QUEUE,
        Ratio0_PRIORITY,
        parameters={
            "targetBands": [
                {
                'name': 's1_ratio',
                'type': 'float32',
                'expression': '(Sigma0_VV_M / Sigma0_VH_M) / (Sigma0_VV_S / Sigma0_VH_S)'
            }
            ]
        }
    )

    diff_op0 = create_operator(
        'BandMaths',
        Diff0,
        BASE_OUT_FOLDER + "/" + Collocate0_OUT,
        BASE_OUT_FOLDER + "/" + Diff0_OUT,
        Diff0_QUEUE,
        Diff0_PRIORITY,
        parameters={
            "targetBands": [
                {
                'name': 's1_diff',
                'type': 'float32',
                'expression': '(Sigma0_VV_M / Sigma0_VH_M) - (Sigma0_VV_S / Sigma0_VH_S)'
            }
            ]
        }
    )

    diffMean_op0 = create_operator(
        'Image-Filter', 
        DiffMean0, 
        BASE_OUT_FOLDER + "/" + Diff0_OUT, 
        BASE_OUT_FOLDER + "/" + DiffMean0_OUT,
        DiffMean0_QUEUE, 
        DiffMean0_PRIORITY,
        parameters={
            'sourceBands': 's1_diff',
            'selectedFilterName': 'Arithmetic 3x3 Mean'
        },
        subprocess=SUBPROCESS
    )

    ratioMean_op0 = create_operator(
        'Image-Filter', 
        RatioMean0, 
        BASE_OUT_FOLDER + "/" + Ratio0_OUT, 
        BASE_OUT_FOLDER + "/" + RatioMean0_OUT,
        RatioMean0_QUEUE, 
        RatioMean0_PRIORITY,
        parameters={
            'sourceBands': 's1_ratio',
            'selectedFilterName': 'Arithmetic 3x3 Mean'
        },
        subprocess=SUBPROCESS
    )

    collocate_op1 = create_operator(
        'Collocate',
        Collocate1,
        [     
            BASE_OUT_FOLDER + "/" + DiffMean0_OUT,
            BASE_OUT_FOLDER + "/" + RatioMean0_OUT
        ],
        BASE_OUT_FOLDER + "/" + Collocate1_OUT,
        Collocate1_QUEUE,
        Collocate1_PRIORITY,
    )

    ellipsoid_correction_op0 >> collocate_op0
    ellipsoid_correction_op1 >> collocate_op0
    collocate_op0 >> diff_op0
    collocate_op0 >> ratio_op0
    diff_op0 >> diffMean_op0 >> collocate_op1
    ratio_op0 >> ratioMean_op0 >> collocate_op1


    ##### S2 #####


    resample_op0 = create_operator(
        'Resample',
        Resample0,
        BASE_OUT_FOLDER + '/' + Download2_OUT + '/' + PRODUCTS[Download2_OUT]['file'],
        BASE_OUT_FOLDER + "/" + Resample0_OUT,
        Resample0_QUEUE,
        Resample0_PRIORITY,
        parameters={
            'referenceBand': 'B8A'
        },
        subprocess=SUBPROCESS
    )

    resample_op1 = create_operator(
        'Resample',
        Resample1,
        BASE_OUT_FOLDER + '/' + Download3_OUT + '/' + PRODUCTS[Download3_OUT]['file'],
        BASE_OUT_FOLDER + "/" + Resample1_OUT,
        Resample1_QUEUE,
        Resample1_PRIORITY,
        parameters={
            'referenceBand': 'B8A'
        },
        subprocess=SUBPROCESS
    )

    collocate_op2 = create_operator(
        'Collocate',
        Collocate2,
        [
            BASE_OUT_FOLDER + "/" + Resample0_OUT, 
            BASE_OUT_FOLDER + "/" + Resample1_OUT
        ],
        BASE_OUT_FOLDER + "/" + Collocate2_OUT,
        Collocate2_QUEUE,
        Collocate2_PRIORITY,
        subprocess=SUBPROCESS
    )

    dNBR_op0 = create_operator(
        'BandMaths',
        dNBR0,
        BASE_OUT_FOLDER + "/" + Collocate2_OUT,
        BASE_OUT_FOLDER + "/" + dNBR0_OUT,
        dNBR0_QUEUE,
        dNBR0_PRIORITY,
        parameters={
            "targetBands": [
                {
                'name': 's2_dNBR',
                'type': 'float64',
                'expression': 'if( scl_vegetation_S or scl_vegetation_M ) then if ( (B12_M - B8A_M) / (B8A_M + B12_M) - (B12_S - B8A_S) / (B8A_S + B12_S) < 0.27 ) then 0 else if ( (B12_M - B8A_M) / (B8A_M + B12_M) - (B12_S - B8A_S) / (B8A_S + B12_S) < 0.66 ) then 0.5 else 1 else NaN'
                }
            ]
        },
        subprocess=SUBPROCESS
    )

    resample_op0 >> collocate_op2
    resample_op1 >> collocate_op2
    collocate_op2 >> dNBR_op0

    ##### CORINE #####

    collocate_op3 = create_operator(
        'Collocate',
        Collocate3,
        [
            BASE_OUT_FOLDER + "/" + dNBR0_OUT, 
            BASE_OUT_FOLDER + "/" + Download4_OUT + '/' + CORINE['file']
        ],
        BASE_OUT_FOLDER + "/" + Collocate3_OUT,
        Collocate3_QUEUE,
        Collocate3_PRIORITY,
        subprocess=SUBPROCESS
    )

    dNBR_op0 >> collocate_op3


    ##### FIRE DETECTION #####

    collocate_op4 = create_operator(
        'Collocate',
        Collocate4,
        [
            BASE_OUT_FOLDER + "/" + Collocate1_OUT, 
            BASE_OUT_FOLDER + "/" + Collocate3_OUT,
        ],
        BASE_OUT_FOLDER + "/" + Collocate4_OUT,
        Collocate4_QUEUE,
        Collocate4_PRIORITY,
        subprocess=SUBPROCESS
    )

    collocate_op3 >> collocate_op4
    collocate_op1 >> collocate_op4

    merge_op0 = create_operator(
        'BandMaths',
        Merge0,
        BASE_OUT_FOLDER + "/" + Collocate4_OUT,
        BASE_OUT_FOLDER + "/" + Merge0_OUT,
        Merge0_QUEUE,
        Merge0_PRIORITY,
        parameters={
            "targetBands": [
                {
                'name': 's2_s1_merge',
                'type': 'float64',
                'expression': 'if(s2_dNBR_M > 0 and (s1_diff_S_S <= -1 or s1_ratio_M_S <= 0.75)) then 1 else 0'
                }
            ]
        },
        subprocess=SUBPROCESS
    )


    noise_op0 = PythonOperator(
        task_id=DeleteNoise0,
        python_callable=delete_noise,
        op_args=[BASE_OUT_FOLDER + "/" + Merge0_OUT.replace('dim', 'data') + '/s2_s1_merge.img', BASE_OUT_FOLDER + '/' + DeleteNoise0_OUT],
        queue=DeleteNoise0_QUEUE,
        priority_weight=DeleteNoise0_PRIORITY,
        weight_rule='absolute'
    )

    collocate_op4 >> merge_op0 >> noise_op0