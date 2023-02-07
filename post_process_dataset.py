import numpy as np
import re
import argparse
from post_process_optics import betasw_ZHH2009
import logging
import xarray as xr
from pathlib import Path
import shutil

from utilities import encode_times

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
                 "processing": "backscatter b_bp calculated following methods in the Ocean Observatories Initiative document "
                               "DATA PRODUCT SPECIFICATION FOR OPTICAL BACKSCATTER (RED WAVELENGTHS) Version 1-05 "
                               "Document Control Number 1341-00540 2014-05-28. Downloaded from "
                               "https://oceanobservatories.org/wp-content/uploads/2015/10/1341-00540_Data_Product_SPEC_FLUBSCT_OOI.pdf"}
    ds["backscatter"] = bbp

    return ds


def post_process(ds):
    _log.info("start post process")
    if "backscatter_scaled" in list(ds):
        ds = calculate_bbp(ds)
    _log.info("complete post process")
    return ds


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Combine ADCP data with glider data')
    parser.add_argument('glider', type=int, help='glider number, e.g. 70')
    parser.add_argument('mission', type=int, help='Mission number, e.g. 23')
    parser.add_argument('kind', type=str, help='Kind of input, must be raw or sub')
    args = parser.parse_args()
    if args.kind not in ['raw', 'sub']:
        raise ValueError('kind must be raw or sub')
    logf = f'/data/log/complete_mission/post_process_SEA{str(args.glider)}_M{str(args.mission)}.log'
    logging.basicConfig(filename=logf,
                        filemode='w',
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        level=logging.INFO,
                        datefmt='%Y-%m-%d %H:%M:%S')
    if args.kind == "raw":
        mtype = "complete_mission"
    else:
        mtype = "nrt"
    nc_path = Path(f"/data/data_l0_pyglider/{mtype}/SEA{args.glider}/M{args.mission}/timeseries/mission_timeseries.nc")
    ds_in = xr.open_dataset(nc_path)
    post_process(ds_in)
    ds_in = encode_times(ds_in)
    tempfile = f"/data/tmp/SEA{args.glider}_M{args.mission}"
    ds_in.to_netcdf(tempfile)
    shutil.move(tempfile, str(nc_path))
