"""
This script checks that the other steps of the pipeline are running as expected.
To monitor a file, add the path to it and part of its expected last line to the files_dict
"""
import datetime
import subprocess
files_dict = {"/data/log/rsync_nrt.log": "total size is",
              "/data/log/pyglider_nrt.log": "Finished nrt processing",
              "/data/log/nrt_plots.log": "End plot creation"}


def get_last_line(file):
    last_line = ""
    with open(file) as f:
        for line in f:
            if len(line) > 5:
                last_line = line
    return last_line


if __name__ == '__main__':
    print(f"{datetime.datetime.now()} start check")
    errors = False
    for filename, string in files_dict.items():
        print(f"Check {filename}")
        final_line = get_last_line(filename)
        if string not in final_line:
            errors = True
            msg = f"failed process: {filename} ends in {final_line}"
            subprocess.check_call(['/usr/bin/bash', "/home/pipeline/utility_scripts/send.sh", msg])
        if errors:
            print(f"{datetime.datetime.now()} failure detected in pipeline ")
            