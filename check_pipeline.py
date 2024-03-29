"""
This script checks that the other steps of the pipeline are running as expected.
To monitor a file, add the path to it and part of its expected last line to the files_dict
"""
import datetime
import subprocess
files_dict = {"/data/log/rsync_nrt.log": "total size is",
              "/data/log/rsync_metocc.log": "total size is",
              "/data/log/rsync_web.log": "total size is",
              "/data/log/erddap_rsync.log": "total size is",
              "/data/log/rsync_sb.log": "total size is",
              "/data/log/pyglider_nrt.log": "Finished nrt processing",
              "/data/log/voto_add_sailbuoy.log": "Finished download of sailbuoy data",
              "/data/log/voto_add_data.log": "nrt mission add complete",
              "/data/log/voto_stats_data.log": "Finished computing stats",
              "/data/log/new_complete_mission.log": "Complete",
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
            subprocess.check_call(['/usr/bin/bash', "/home/pipeline/utility_scripts/send.sh", msg, "pipeline-clogged", "callum.rollo@voiceoftheocean.org"])
        if errors:
            print(f"{datetime.datetime.now()} failure detected in pipeline ")
            