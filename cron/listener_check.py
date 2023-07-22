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
    scripts = [
        {
            "name"     : "alert_listener",
            "script"   : f"{home_path}/git-clones/gwtm_cron/cron/gwtm_listener_run.py",
            "log_file" : f"{home_path}/cron/listener.log"
        }, 
        {
            "name"     : "icecube_listener",
            "script"   : f"{home_path}/git-clones/gwtm_cron/cron/gwtm_icecube_run.py",
            "log_file" : f"{home_path}/cron/listener.log"
        },
    ]

    for script in scripts:
        listener = script["script"]
        name = script["name"]
        log_file = script["log_file"]

        if not checkIfProcessRunning(listener):
            print(f"Listener: {name} not running! restarting", datetime.datetime.now())   
            with open(log_file, "a") as out:
                subprocess.Popen([python_path, listener], stdout=out)

        else:
            print(f"Listener: {name} is running, nothing to worry about", datetime.datetime.now())
