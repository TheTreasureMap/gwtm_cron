from gwtm_cron import gwtm_listener

if __name__ == "__main__":
    l = gwtm_listener.listener.Listener(listener_type="ICECUBE_NOTICE", config_path="/home/azureuser/cron/listener_config.json")
    l.run(write_to_s3=True, verbose=True, dry_run=False)


