import numpy as np
import re
from post_process_optics import betasw_ZHH2009
import logging
from votoutils.utilities.geocode import filter_territorial_data
from post_process_ctd import salinity_pressure_correction, correct_rbr_lag
_log = logging.getLogger(__name__)


def calculate_bbp(ds, beam_angle=117):
    # https://oceanobservatories.org/wp-content/uploads/2015/10/1341-00540_Data_Product_SPEC_FLUBSCT_OOI.pdf
    _log.info("processing backscatter")
    temperature = ds["temperature"].values
    salinity = ds["salinity"].values
    beta_total = ds["backscatter_scaled"].values
    backscatter_str = ds["backscatter_scaled"].attrs["standard_name"]
    wavelength = int(re.findall(r'\d+', backscatter_str)[0])
    beta_sw, __, __ = betasw_ZHH2009(temperature, salinity, wavelength, beam_angle)
    beta_p = beta_total - beta_sw
    if beam_angle == 117:
        chi_p = 1.08  # For 117* angle (Sullivan & Twardowski, 2009)
    elif beam_angle == 140:
        chi_p = 1.17  # For 140* angle (Sullivan & Twardowski, 2009)
    else:
        _log.error(f"Incompatible beam_angle. Allowed values are 117 or 140")
        return
    bbp_val = 2 * np.pi * chi_p * beta_p  # in m-1
    bbp = ds["backscatter_scaled"].copy()
    bbp.values = bbp_val
    bbp.attrs = {"units": "m^{-1}",
                 'observation_type': 'calculated',
                 'standard_name': f'{wavelength}_nm_scattering_of_particles_integrated_over_the_backwards hemisphere',
                 "long_name": f"{wavelength} nm b_bp: scattering of particles integrated over the backwards hemisphere",
                 "processing": "Particulate backscatter b_bp calculated following methods in the Ocean Observatories Initiative document "
                               "DATA PRODUCT SPECIFICATION FOR OPTICAL BACKSCATTER (RED WAVELENGTHS) Version 1-05 "
                               "Document Control Number 1341-00540 2014-05-28. Downloaded from "
                               "https://oceanobservatories.org/wp-content/uploads/2015/10/1341-00540_Data_Product_SPEC_FLUBSCT_OOI.pdf"}
    ds["particulate_backscatter"] = bbp

    return ds


def vertical_distance_from_altimeter(altimeter, pitch_glider, roll):
    pitch_altimeter = pitch_glider + 20
    vertical_distance = np.cos(np.deg2rad(pitch_altimeter)) * np.cos(np.deg2rad(roll)) * altimeter
    return vertical_distance


def process_altimeter(ds):
    """
    From the seaexploer manual: the angle of the altimeter is 20 degrees, such that it is vertical when the glider
    is pitched at 20 degrees during the dive.
    :param ds:
    :return: ds with additional bathymetry variable
    """
    if "altimeter" not in list(ds):
        _log.warning("No altimeter data found")
        return ds
    altim_raw = ds["altimeter"].values
    altim = altim_raw.copy()
    altim[altim_raw <= 0] = np.nan
    bathy_from_altimeter = vertical_distance_from_altimeter(altim, ds["pitch"].values, ds["roll"].values)
    vertical_distance_to_seafloor = ds["altimeter"].copy()
    vertical_distance_to_seafloor.values = bathy_from_altimeter
    attrs = vertical_distance_to_seafloor.attrs
    attrs["long_name"] = "vertical distance from glider to seafloor"
    attrs["standard_name"] = "vertical_distance_to_seafloor"
    attrs["comment"] = "Distance to the seafloor is calculated from the glider altimeter (see altimeter variable)," \
                       " which is oriented at 20 degrees from the vertical such that it is vertical when the glider " \
                       "is pitched downwards at 20 degrees."
    vertical_distance_to_seafloor.attrs = attrs
    ds["vertical_distance_to_seafloor"] = vertical_distance_to_seafloor
    return ds


def fix_variables(ds):
    attrs = ds.attrs
    if int(attrs["glider_serial"]) == 69 and int(attrs["deployment_id"]) == 15:
        _log.info("correcting phycocyanin values for SEA69 M15")
        ds["phycocyanin"].values = ds["phycocyanin"].values * 0.1
        ds.phycocyanin.attrs["comment"] += (" Values multiplied by 0.1 in post-processing to correct for bad scale "
                                            "factor during deployment")
    return ds


def nan_bad_depths(ds):
    ds['depth'][ds['depth'] > int(ds['depth'].attrs['valid_max'])] = np.nan
    ds['pressure'][ds['pressure'] > int(ds['pressure'].attrs['valid_max'])] = np.nan
    return ds


def nan_bad_locations(ds):
    ds['longitude'].values[ds['longitude_qc'] > 3] = np.nan
    ds['latitude'].values[ds['latitude_qc'] > 3] = np.nan
    return ds


def post_process(ds):
    _log.info("start post process")
    ds = salinity_pressure_correction(ds)
    ds = correct_rbr_lag(ds)
    ds = process_altimeter(ds)
    ds = filter_territorial_data(ds)
    if "backscatter_scaled" in list(ds):
        ds = calculate_bbp(ds)
    ds = fix_variables(ds)
    ds = nan_bad_depths(ds)
    ds = nan_bad_locations(ds)
    ds = ds.sortby("time")
    _log.info("complete post process")
    return ds

