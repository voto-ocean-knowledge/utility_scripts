import re
import numpy as np
import logging
_log = logging.getLogger(__name__)


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


def encode_times(ds):
    if 'units' in ds.time.attrs.keys():
        ds.time.attrs.pop('units')
    if 'calendar' in ds.time.attrs.keys():
        ds.time.attrs.pop('calendar')
    ds["time"].encoding["units"] = 'seconds since 1970-01-01T00:00:00Z'
    if 'ad2cp_time' in list(ds):
        if 'units' in ds.ad2cp_time.attrs.keys():
            ds.ad2cp_time.attrs.pop('units')
        if 'calendar' in ds.ad2cp_time.attrs.keys():
            ds.time.attrs.pop('calendar')
        cal_str = 'seconds since 1970-01-01T00:00:00Z'
        ds["ad2cp_time"].encoding["units"] = cal_str
    return ds


def find_best_dtype(var_name, da):
    input_dtype = da.dtype.type
    if var_name[-2:] == "qc":
        return np.int8
    if var_name[-3:] == "raw":
        input_dtype = np.int32
    if "int" in str(input_dtype):
        if max(da.values) < 2**16 / 2:
            return np.int16
        elif max(da.values) < 2**32 / 2:
            return np.int32
    if input_dtype == np.float64:
        return np.float32
    return input_dtype


def set_best_dtype(ds):
    bytes_in = ds.nbytes
    for var_name in list(ds):
        da = ds[var_name]
        input_dtype = da.dtype.type
        new_dtype = find_best_dtype(var_name, da)
        if new_dtype == input_dtype:
            continue
        _log.info(f"{var_name} input dtype {input_dtype} change to {new_dtype}")
        da_new = da.astype(new_dtype)
        ds = ds.drop_vars(var_name)
        ds[var_name] = da_new
    bytes_out = ds.nbytes
    _log.info(f"Space saved by dtype downgrade: {int(100 * (bytes_in - bytes_out) / bytes_in)} %")
    return ds
