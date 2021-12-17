import os
import sys
import pathlib
import argparse
import numpy as np
import logging
import glob
import shutil
import re
import xarray as xr


script_dir = pathlib.Path(__file__).parent.absolute()
sys.path.append(str(script_dir))
os.chdir(script_dir)
from process_pyglider import proc_pyglider_l0

_log = logging.getLogger(__name__)
logging.basicConfig(filename='/data/log/pyglider_complete_mission.log',
                    filemode='a',
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    level=logging.INFO,
                    datefmt='%Y-%m-%d %H:%M:%S')


def natural_sort(unsorted_list):
    convert = lambda text: int(text) if text.isdigit() else text.lower()
    alphanum_key = lambda key: [convert(c) for c in re.split('([0-9]+)', key)]
    return sorted(unsorted_list, key=alphanum_key)


def match_input_files(gli_infiles, pld_infiles):
    gli_nums = []
    for fname in gli_infiles:
        parts = fname.split('.')
        try:
            gli_nums.append(int(parts[-1]))
        except ValueError:
            try:
                gli_nums.append(int(parts[-2]))
            except ValueError:
                raise ValueError("Unexpected gli file filename found in input. Aborting")
    pld_nums = []
    for fname in pld_infiles:
        parts = fname.split('.')
        try:
            pld_nums.append(int(parts[-1]))
        except ValueError:
            try:
                pld_nums.append(int(parts[-2]))
            except ValueError:
                raise ValueError("Unexpected pld file filename found in input. Aborting")
    good_cycles = set(pld_nums) & set(gli_nums)
    good_gli_files = []
    good_pld_files = []
    for i, num in enumerate(gli_nums):
        if num in good_cycles:
            good_gli_files.append(gli_infiles[i])
    for i, num in enumerate(pld_nums):
        if num in good_cycles:
            good_pld_files.append(pld_infiles[i])
    return good_gli_files, good_pld_files


def batched_process(args):
    if args.batchsize:
        batch_size = args.batchsize
    else:
        batch_size = 500
    if args.steps:
        steps = [int(item) for item in args.steps.split(',')]
    else:
        steps = [1, 1, 1, 1]

    # Process in batches of dives (default 500) to avoid maxxing out memory
    input_dir = f"/data/data_raw/complete_mission/SEA{args.glider}/M{args.mission}/"
    if not input_dir:
        raise ValueError(f"Input dir {input_dir} not found")
    output_dir = f"/data/data_l0_pyglider/complete_mission/SEA{args.glider}/M{args.mission}/"

    in_files_gli = natural_sort(glob.glob(f"{input_dir}*gli*"))
    in_files_pld = natural_sort(glob.glob(f"{input_dir}*pld*{args.kind}*"))
    in_files_gli, in_files_pld = match_input_files(in_files_gli, in_files_pld)

    if len(in_files_gli) == 0 or len(in_files_pld) == 0:
        raise ValueError(f"input dir {input_dir} does not contain gli and/or pld files")
    num_batches = int(np.ceil(len(in_files_gli) / batch_size))
    _log.info(f"Processing glider {args.glider} mission {args.mission} in {num_batches} batches")

    # If less than one batch worth, process directly
    if num_batches == 1:
        proc_pyglider_l0(args.glider, args.mission, args.kind, input_dir, output_dir, steps=steps)
        _log.info(f"Finished processing glider {args.glider} mission {args.mission} in {num_batches} batch")
        return
    # Process input files in batches
    for i in range(num_batches):
        start = i * batch_size
        end = (i + 1) * batch_size
        in_sub_dir = f"{input_dir[:-1]}_sub_{i}/"
        if not pathlib.Path(in_sub_dir).exists():
            pathlib.Path(in_sub_dir).mkdir(parents=True)
        in_files_gli_sub = in_files_gli[start:end]
        in_files_pld_sub = in_files_pld[start:end]
        # Copy input into a sub directory
        for filename in in_files_gli_sub:
            shutil.copy(filename, in_sub_dir)
        for filename in in_files_pld_sub:
            shutil.copy(filename, in_sub_dir)
        # create output directory
        out_sub_dir = f"{output_dir[:-1]}_sub_{i}/"
        if not pathlib.Path(out_sub_dir).exists():
            pathlib.Path(out_sub_dir).mkdir(parents=True)
        # Process on sub-directory
        _log.info(f"Started processing glider {args.glider} mission {args.mission} batch {i}")
        proc_pyglider_l0(args.glider, args.mission, args.kind, in_sub_dir, out_sub_dir, steps=steps)
        _log.info(f"Finished processing glider {args.glider} mission {args.mission} batch {i}")
    
    # Recombine outputs
    _log.info(f"Recombining glider {args.glider} mission {args.mission} from {num_batches} batches")
    if not pathlib.Path(output_dir).exists():
        pathlib.Path(output_dir).mkdir(parents=True)
    rawncdir = output_dir + 'rawnc/'
    l0tsdir = output_dir + 'timeseries/'
    profiledir = output_dir + 'profiles/'
    griddir = output_dir + 'gridfiles/'
    if not pathlib.Path(rawncdir).exists():
        pathlib.Path(rawncdir).mkdir(parents=True)
    if not pathlib.Path(profiledir).exists():
        pathlib.Path(profiledir).mkdir(parents=True)
    _log.info(f"Copying rawnc and l0 profiles files")
    for i in range(num_batches):
        out_sub_dir = f"{output_dir[:-1]}_sub_{i}/"
        sub_rawncdir = out_sub_dir + 'rawnc/'
        sub_profiledir = out_sub_dir + 'profiles/'
        in_raw_nc = glob.glob(f"{sub_rawncdir}*.nc")
        for filename in in_raw_nc:
            shutil.copy(filename, rawncdir)
        in_profile = glob.glob(f"{sub_profiledir}*.nc")
        for filename in in_profile:
            shutil.copy(filename, profiledir)
    _log.info('Recombining timeseries and grid ncs')
    sub_timeseries = []
    sub_gridfiles = []
    for i in range(num_batches):
        out_sub_dir = f"{output_dir[:-1]}_sub_{i}/"
        sub_timeseries.append(glob.glob(f"{out_sub_dir}timeseries/*.nc")[0])
        sub_gridfiles.append(glob.glob(f"{out_sub_dir}gridfiles/*.nc")[0])

    mission_timeseries = xr.open_mfdataset(sub_timeseries, combine='by_coords', decode_times=False)
    mission_timeseries.load()
    mission_timeseries.to_netcdf(f"{l0tsdir}mission_timeseries.nc")

    mission_grid = xr.open_mfdataset(sub_gridfiles, combine='by_coords', decode_times=False)
    mission_grid.load()
    mission_grid.to_netcdf(f"{griddir}mission_grid.nc")
    _log.info('Recombination complete')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='process SX files with pyglider')
    parser.add_argument('glider', type=int, help='glider number, e.g. 70')
    parser.add_argument('mission', type=int, help='Mission number, e.g. 23')
    parser.add_argument('kind', type=str, help='Kind of input, must be raw or sub')
    parser.add_argument('--steps', type=str, help='List of steps to perform. 1 performs step, 0 skips it. e.g. "0, 0, '
                                                  '1, 1" will skip delete old data and convert rawnc steps. '
                                                  'Will perform the merge_rawnc, create l0 products')
    parser.add_argument('--batchsize', type=int, help='Number of dives to process per batch. Defaults to 500. Reduce '
                                                      'this number if processing is maxxing out memory. Processed '
                                                      'datasets are recombined at the end')
    args = parser.parse_args()
    if args.kind not in ['raw', 'sub']:
        raise ValueError('kind must be raw or sub')
    batched_process(args)
