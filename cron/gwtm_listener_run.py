from gwtm_cron import gwtm_listener

if __name__ == "__main__":
    l = gwtm_listener.listener.Listener()
    l.run(write_to_s3=True, verbose=True, dry_run=False)


