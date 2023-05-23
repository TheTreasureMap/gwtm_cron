import psutil
import os
import subprocess
import datetime

def checkIfProcessRunning(processName):
    '''
    Check if there is any running process that contains the given name processName.
    '''
    processes = filter(lambda p: psutil.Process(p).name() == "python", psutil.pids())
    scripts = []
    paths = []
    for pid in processes:
        try:
            scripts.append(psutil.Process(pid).cmdline()[1])
        except IndexError:
            pass

    return processName in scripts

if __name__ == '__main__':

    home_path   = "/home/azureuser"
    python_path = f"{home_path}/anaconda3/envs/gwtm_listener/bin/python"
    listener    = f"{home_path}/git-clones/gwtm_cron/cron/gwtm_listener_run.py"
    log_file    = f"{home_path}/cron/listener.log"

    if not checkIfProcessRunning(listener):
        print("Not running! restarting", datetime.datetime.now())   
        with open(log_file, "a") as out:
            subprocess.Popen([python_path, listener], stdout=out)
        #subprocess.Popen([python_path, listener, ">>", log_file, "2>&1"])

    else:
        print("Listener is running, nothing to worry about", datetime.datetime.now())
