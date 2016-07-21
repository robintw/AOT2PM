import numbers
import gdal
import numpy as np

import os
import tempfile
from subprocess import call
import logging
from resample_image import resample_raster

from joblib import Memory

memory = Memory(cachedir='.', verbose=1)

#@memory.cache(ignore=['aot_filename'])
def get_eta_data_wrapper(aot_filename, eta_filename, month, geotransform, proj):
    eta_band = tempfile.NamedTemporaryFile(prefix='hotbar', delete=False).name
    eta_band_resampled = tempfile.NamedTemporaryFile(prefix='hotbar', delete=False).name

    command = 'gdal_translate -b %d %s %s' % (month, eta_filename, eta_band)

    logging.debug(command)
    return_code = call(command, shell=True)
    if return_code != 0:
        logging.warn('gdal_translate command returned %d' % return_code)

    eta_img = resample_raster(eta_band, aot_filename, eta_band_resampled)
    eta_gdal = gdal.Open(eta_band_resampled)
    eta_data = eta_gdal.GetRasterBand(1).ReadAsArray()

    # Delete temporary files
    os.remove(eta_band)
    os.remove(eta_band_resampled)

    return eta_data


def get_eta_data(aot_filename, eta_filename, month):
    aot_im = gdal.Open(aot_filename)

    result = get_eta_data_wrapper(aot_filename, eta_filename, month,
                                  aot_im.GetGeoTransform(),
                                  aot_im.GetProjection())
    aot_im = None

    return result

def convert_to_pm25(aot_filename, eta_filename, month, dest_filename, aot_scale_factor=1):
    """Converts a raster file containing AOT data to PM2.5 using the van Donkelaar
    conversion factors, and stores this in an output raster file.

    Parameters
    ----------
    aot_filename : str
        Filename of raster file containing the AOT data. Must be in a GDAL-
        compatible format
    eta_filename : str
        Filename of the eta_Monthly.tif file provided by van Donkelaar
    month: int
        Month the AOT data was acquired in (1 = Jan, 2 = Feb etc)
    dest_filename: str
        Filename to store output PM2.5 image in. Will use the same format (eg. GeoTIFF)
        as aot_filename.
    aot_scale_factor: float, optional
        Scaling factor used to convert the AOT values in the input file into
        real AOT units (eg. 0.001 to divide all AOT values by 1000). Defaults to 1,
        which does not alter the input AOT values.
    """
    if not isinstance(month, numbers.Integral):
        raise TypeError('Parameter `month` must be integer')

    if not 1 <= month <= 12:
        raise TypeError('Parameter `month` must be integer between 1 and 12')

    if not os.path.exists(eta_filename):
        raise ValueError('eta_filename must be a valid path to the eta_monthly file')

    aot_gdal = gdal.Open(aot_filename)
    aot_nodata_value = aot_gdal.GetRasterBand(1).GetNoDataValue()

    aot_data = aot_gdal.GetRasterBand(1).ReadAsArray()
    aot_data[aot_data == aot_nodata_value] = np.nan

    eta_data = get_eta_data(aot_filename, eta_filename, month)

    pm25_data = aot_data * aot_scale_factor * eta_data

    drv = aot_gdal.GetDriver()
    pm25_im = drv.CreateCopy(dest_filename, aot_gdal)

    # Write the image out
    pm25_im.GetRasterBand(1).WriteArray(pm25_data)

    pm25_im = None

    return pm25_data
