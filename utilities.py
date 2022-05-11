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


def bump_up(profile_nums, chunk_size):
    for i, num in enumerate(profile_nums[1:]):
        if num < profile_nums[i]:
            profile_nums[i + 1] += chunk_size
    return profile_nums


def fix_profile_number(ds, var_name='profile'):
    profile_nums_raw = ds[var_name].values
    profile_nums = profile_nums_raw[np.logical_and(profile_nums_raw != 0, ~np.isnan(profile_nums_raw))]
    step = np.max(profile_nums)
    profile_nums_pre = profile_nums - 1
    while (profile_nums != profile_nums_pre).any():
        _log.info(f"step found. Bump up {step}")
        profile_nums_pre = profile_nums.copy()
        profile_nums = bump_up(profile_nums, step)
    profile_nums_raw[np.logical_and(profile_nums_raw != 0, ~np.isnan(profile_nums_raw))] = profile_nums
    ds[var_name].values = profile_nums_raw
    return ds
