from pathlib import Path
import datetime
import pyglider_all_complete_missions


if __name__ == '__main__':
    logf = Path(f'/data/log/complete_mission_reprocess.log')
    mtime = datetime.datetime.fromtimestamp(logf.lstat().st_mtime)
    time_elapsed = datetime.datetime.now() - mtime
    if time_elapsed > datetime.timedelta(hours=1):
        pyglider_all_complete_missions.main()
