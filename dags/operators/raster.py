# access to geospatial raster data
import logging
from glob import glob

log = logging.getLogger(__name__)

def collect_bands(input, output):
    import numpy as np
    import rasterio
    bands_map = {
        # 'blue': glob(input + '/**/*B02*.jp2', recursive=True)[0],
        # 'green': glob(input + '/**/*B03*.jp2', recursive=True)[0],
        'red': glob(input + '/**/*B04*.jp2', recursive=True)[0],
        'nir': glob(input + '/**/*B08*.jp2', recursive=True)[0],
        'cloud': glob(input + '/**/MSK_CLDPRB_60m.jp2', recursive=True)[0]
    }

    refBand = rasterio.open(bands_map['red']) 
    out_meta = {
        'driver': 'GTiff',
        'width': refBand.width,
        'height': refBand.height,
        'count': 3,
        'crs': refBand.crs,
        'transform': refBand.transform,
        'dtype': refBand.dtypes[0]
    }
    refBand.close()

    with rasterio.open(output,'w', **out_meta) as out:
        counter = 1
        for band_id,band_path in bands_map.items():
            with rasterio.open(band_path) as band:
                if band_id == 'cloud':
                    band_mask = load_cloud_mask(band, out_meta['width'], out_meta['height'])
                    out.write(band_mask, counter)
                else:
                    out.write(band.read(1), counter)
            counter += 1

def load_cloud_mask(msk, refWidth, refHeight):
    import numpy as np
    import rasterio
    # resample
    prb_msk = msk.read(
        out_shape=(refHeight, refWidth),
        resampling=rasterio.enums.Resampling.bilinear
    )
    # transform to numpy mask
    np_msk = np.where(prb_msk > 0, 0, 1)
    # out_meta = {
    #     'driver': 'GTiff',
    #     'width': refWidth,
    #     'height': refHeight,
    #     'count': 1,
    #     'crs': msk.crs,
    #     'transform': msk.transform,
    #     'dtype': msk.dtypes[0]
    # }

    return np_msk[0]


def calc_ndvi(input, output, mask = False):
    import rasterio
    import numpy as np

    with rasterio.open(input) as src:
        bandRed = src.read(1)
        bandNir = src.read(2)
        if mask:
            bandMask = src.read(3)
            bandNir = np.ma.array(bandNir, mask=bandMask)
            bandRed = np.ma.array(bandRed, mask=bandMask)

        ndvi = (bandNir.astype(float)-bandRed.astype(float))/(bandNir+bandRed)
        kwargs = src.meta.copy()
        kwargs.update(
            dtype=rasterio.float32,
            count=1)
    
    with rasterio.open(output, 'w', **kwargs) as dst:
        dst.write_band(1, ndvi.astype(rasterio.float32))

def calc_ndvi_variations(input1, input2, output):
    import numpy as np
    import rasterio
    with rasterio.open(input1) as src:
        master_ndvi = src.read(1)
        kwargs = src.meta.copy()

    with rasterio.open(input2) as src:
        slave_ndvi = src.read(1)

    difference_ndvi = (master_ndvi.astype(float)-slave_ndvi.astype(float))
    kwargs.update(
        dtype=rasterio.float32,
        count=1)
    
    with rasterio.open(output, 'w', **kwargs) as dst:
        dst.write_band(1, difference_ndvi.astype(rasterio.float32))


def delete_noise(product, output):
    import rasterio
    import pandas as pd
    import geopandas as gp
    from pyproj import Proj, Transformer, CRS
    from scipy.sparse import coo_matrix 
    from sklearn.cluster import DBSCAN

    # read data
    with rasterio.open(product) as product:
        # read first band inside product
        band = product.read(1)
        # convertion to matrix obj in coordinates format i,j,v
        matrix = coo_matrix(band)
        # extract list of points
        xy_points = list(zip(matrix.col, matrix.row)) # x-axis goes from left to right (cols); y-axis goes from top to bottom (rows)
        # transform pixel coordinates to lat lon coordinates
        crs_from = CRS(product.crs.to_string())
        crs_to = CRS('EPSG:3857')
    
    # transform data
    affine = product.transform # pixel to world coord transformation
    t1 = Transformer.from_crs(crs_from, crs_to, always_xy=True) # product epsg to google maps epsg transformation
    t2 = Transformer.from_crs(crs_to, crs_to.geodetic_crs, always_xy=True) # google maps epsg to geodetic
    points = []
    for x,y in xy_points:
        ax, ay = affine * (x,y)
        t1x, t1y = t1.transform(ax, ay)
        t2x, t2y = t2.transform(t1x, t1y)
        lat = t2y
        lon = t2x
        points.append((x,y,lat,lon))
    df = pd.DataFrame(points, columns=['x','y','lat','lon'])
    gdf = gp.GeoDataFrame(df, geometry=gp.points_from_xy(df.lon, df.lat))
    
    # find clusters
    dbscan_model = DBSCAN(eps=5, min_samples=5).fit(xy_points) # DBSCAN clustering
    gdf['label'] = dbscan_model.labels_ # assign cluster labels to points

    # delete noise
    gdf.drop(gdf[gdf.label == -1].index, inplace=True) # delete isolated points
    subset = gdf # no subset
    # subset = gdf.loc[df['label'].isin(gdf['label'].value_counts().index.to_numpy()[:100])] # subset to first N larger clusters
    
    # create convex hull around clusters
    geometries = []
    for clusterid, frame in subset.groupby('label'): 
        geom = frame.geometry.unary_union.convex_hull # create one multipoint and convex hull it
        geometries.append([clusterid, geom])
    hulls = gp.GeoDataFrame(geometries, columns=['label', 'geometry'])

    with open(output, 'w') as f:
        f.write(hulls.to_json())