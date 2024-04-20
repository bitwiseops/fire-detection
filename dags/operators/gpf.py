import logging
import multiprocessing
from airflow.operators.python import PythonOperator
from multiprocessing import Process, Pipe

log = logging.getLogger(__name__)

# SHARED

def _read(input, printer):
    import snappy, jpy

    if isinstance(input, list):
        printer("Reading multiple sources...")
        sources = []
        for i in input:
            printer(i)
            sources.append(snappy.ProductIO.readProduct(i))
        return sources
    else:
        printer("Reading: " + input)
        source = snappy.ProductIO.readProduct(input)
        printer("Source ready")
        return source

def _write(target, output, printer):
    import snappy, jpy

    printer("Writing: " + output)
    snappy.ProductIO.writeProduct(target, output, 'BEAM-DIMAP')
    printer("Product saved")

def _init_params(dict, printer):
    import snappy, jpy

    parameters = snappy.HashMap()
    if dict:
        printer("Found params...")
        for key, value in dict.items():
            if key == 'targetBands':
                printer("Creating bands...")
                value = create_bands(value)
            else:
                printer("Adding ("+key+","+value +")")
            parameters.put(key, value)
    else:
        printer("No params supplied.")
    return parameters

def create_bands(bandsParams):
    import snappy, jpy

    BandDescriptor = jpy.get_type('org.esa.snap.core.gpf.common.BandMathsOp$BandDescriptor')
    targetBands = jpy.array('org.esa.snap.core.gpf.common.BandMathsOp$BandDescriptor', len(bandsParams))

    for i,bandParams in enumerate(bandsParams):
        targetBand = BandDescriptor()
        targetBand.name = bandParams['name']
        targetBand.type = bandParams['type']
        targetBand.expression = bandParams['expression']
        # targetBand.noDataValue = 0
        targetBands[i] = targetBand

    return targetBands



def do_operation(name, input, output, parameters, pipe = None):
    import snappy, jpy

    if pipe:
        printer = getattr(pipe, "send")
    else:
        printer = getattr(log, "info")
    try:
        printer(f"Received inputs: {name}, {input}, {output}, {str(parameters)}"  )
        sources = _read(input, printer)
        parameters = _init_params(parameters, printer)
        printer('Executing: ' + name)
        target = snappy.GPF.createProduct(name, parameters, sources)
        _write(target, output, printer)
    except Exception as e:
        printer('EXCEPTION')
        printer(str(e))
        raise e
    finally:
        printer('DONE')
        if pipe:
            pipe.close()

def do_subprocess_operation(name, input, output, parameters):
    multiprocessing.set_start_method("spawn")
    (pipe_in, pipe_out) = Pipe()
    p = Process(target=do_operation, args=(name, input, output, parameters, pipe_in))
    log.info("Starting subprocess execution...")
    p.start()
    while True:
        msg = pipe_out.recv()
        log.info(msg)
        if msg == 'EXCEPTION':
            raise Exception(pipe_out.recv())
        if msg == 'DONE':
            break
    p.join()
    pipe_out.close()


def create_operator(name, task_id, input, output, queue, priority_weight, parameters={}, subprocess=True):
    return PythonOperator(
        task_id=task_id,
        python_callable=do_operation if not subprocess else do_subprocess_operation,
        op_args=[name, input, output, parameters],
        queue=queue,
        priority_weight=priority_weight,
    )


# TEST
if __name__ == '__main__':
    input = '/data/speckle.dim'
    output = '/data/terrain.dim'
    parameters = {
            'demName': 'SRTM 3Sec',
            'externalDEMNoDataValue': '0.0',
            'externalDEMApplyEGM': 'true',
            'demResamplingMethod': 'BILINEAR_INTERPOLATION',
            'imgResamplingMethod': 'BILINEAR_INTERPOLATION',
            'pixelSpacingInMeter': '10.0',
            'pixelSpacingInDegree': '8.983152841195215E-5',
            'mapProjection': '''GEOGCS["WGS84(DD)", 
  DATUM["WGS84", 
    SPHEROID["WGS84", 6378137.0, 298.257223563]], 
  PRIMEM["Greenwich", 0.0], 
  UNIT["degree", 0.017453292519943295], 
  AXIS["Geodetic longitude", EAST], 
  AXIS["Geodetic latitude", NORTH]]''',
            'alignToStandardGrid': 'false',
            'standardGridOriginX': '0.0',
            'standardGridOriginY': '0.0',
            'nodataValueAtSea': 'true',
            'saveDEM': 'false',
            'saveLatLon': 'false',
            'saveIncidenceAngleFromEllipsoid': 'false',
            'saveLocalIncidenceAngle': 'false',
            'saveProjectedLocalIncidenceAngle': 'false',
            'saveSelectedSourceBand': 'true',
            'outputComplex': 'false',
            'applyRadiometricNormalization': 'false',
            'saveSigmaNought': 'false',
            'saveGammaNought': 'false',
            'saveBetaNought': 'false',
            'incidenceAngleForSigma0': 'Use projected local incidence angle from DEM',
            'incidenceAngleForGamma0': 'Use projected local incidence angle from DEM',
            'auxFile': 'Latest Auxiliary File'
        }

    do_subprocess_operation('Terrain-Correction', input, output, parameters)