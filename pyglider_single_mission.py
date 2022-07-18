import os
import sys
import pathlib
import argparse
import numpy as np
import logging
import glob
import shutil
import pandas as pd
import datetime
import subprocess

script_dir = pathlib.Path(__file__).parent.absolute()
parent_dir = script_dir.parents[0]
sys.path.append(str(script_dir))
os.chdir(script_dir)
from utilities import natural_sort, match_input_files
from process_pyglider import proc_pyglider_l0
from recombine import recombine
from geocode import update_ncs

_log = logging.getLogger(__name__)


def batched_process(args):
    if args.batchsize:
        batch_size = args.batchsize
    else:
        batch_size = 100
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
    num_files = len(in_files_gli)
    starts = np.arange(0, num_files, batch_size)
    ends = np.arange(batch_size, num_files + batch_size , batch_size)
    # fix for if we have 2 or fewer files in final batch
    if num_files - starts[-1] < 3:
        starts = starts[:-1]
        ends[-2] = ends[-1]
        ends = ends[:-1]
        _log.info(f"reduced to {len(starts)} batches")
    for i in range(len(starts)):
        start = starts[i]
        end = ends[i]
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

    _log.info('Batched processing complete')


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
    logf = f'/data/log/complete_mission/SEA{str(args.glider)}_M{str(args.mission)}.log'
    logging.basicConfig(filename=logf,
                        filemode='w',
                        format='%(asctime)s %(levelname)-8s %(message)s',
                        level=logging.INFO,
                        datefmt='%Y-%m-%d %H:%M:%S')
    batched_process(args)
    mission_dir = pathlib.Path(f"/data/data_l0_pyglider/complete_mission/SEA{args.glider}/M{args.mission}")
    if mission_dir.exists():
        shutil.rmtree(mission_dir)
    recombine(args.glider, args.mission)
    # Call follow-up scripts
    if args.kind == "raw":
        subprocess.check_call(['/usr/bin/bash', "/home/pipeline/utility_scripts/clean_mission.sh", str(args.glider), str(args.mission)])
        update_ncs(args.glider, args.mission, 'complete_mission')
        sys.path.append(str(parent_dir / "quick-plots"))
        # noinspection PyUnresolvedReferences
        from complete_mission_plots import complete_plots
        complete_plots(args.glider, args.mission)
        _log.info("Finished plot creation")
        sys.path.append(str(parent_dir / "voto-web/voto/bin"))
        # noinspection PyUnresolvedReferences
        from add_profiles import init_db, add_complete_profiles
        init_db()
        add_complete_profiles(pathlib.Path(f"/data/data_l0_pyglider/complete_mission/SEA{args.glider}/M{args.mission}"))
        df_reprocess = pd.read_csv('/home/pipeline/reprocess.csv')
        a = [np.logical_and(df_reprocess.glider == args.glider, df_reprocess.mission == args.mission)]
        ind = df_reprocess.index[tuple(a)].values[0]
        df_reprocess.at[ind, "proc_time"] = datetime.datetime.now()
        _log.info(f"updated processing time to {datetime.datetime.now()}")
        df_reprocess.to_csv('/home/pipeline/reprocess.csv', index=False)
        _log.info("Finished add to database")
