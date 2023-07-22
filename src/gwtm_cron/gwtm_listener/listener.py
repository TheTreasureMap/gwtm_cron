

from gcn_kafka import Consumer

try:
    from . import gw_config as config
    from . import ligo_alert
    from . import icecube_notice
except:
    print("import error?")
    import gw_config as config
    import ligo_alert
    import icecube_notice

LISTENER_TYPES = {
    "LIGO_ALERT" : { 
        "func" : ligo_alert.listen, 
        "domain": "igwn.gwalert"
    },
    "ICECUBE_NOTICE" : {
        "func" : icecube_notice.listen,
        "domain" : "gcn.notices.icecube.lvk_nu_track_search",
    }
}


class Listener():

    def __init__(self, listener_type):

        assert listener_type in LISTENER_TYPES.keys(), "Invalid Listener Type"

        self.listener_type = listener_type
        home = "/home/azureuser"
        home = "/Users/crisp"
        conf_path = "/cron/listener_config.json"

        self.config = config.Config(path_to_config=f"{home}{conf_path}")

        self.consumer = Consumer(
            client_id=self.config.KAFKA_CLIENT_ID,
            client_secret=self.config.KAFKA_CLIENT_SECRET
        )
        
        self.consumer.subscribe([
            LISTENER_TYPES[self.listener_type]["domain"]
        ])


    def _listen(self, alert, write_to_s3, verbose, dry_run, alertname):
        listener_function = LISTENER_TYPES[self.listener_type]["func"]
        return listener_function(self.config, alert, write_to_s3, verbose, dry_run, alertname)
    

    def run(self, write_to_s3=True, verbose=False, dry_run=False):
        if verbose:
            print(f'Listening for alerts from {self.config.ALERT_DOMAIN}')

        while True:
            for message in self.consumer.consume(timeout=1):
                alert, ext_alert = self._listen(
                    alert=message.value(), 
                    write_to_s3=write_to_s3,
                    verbose=verbose,
                    dry_run=dry_run
                )
                if verbose:
                    print(alert)
                    if ext_alert:
                        print()
                        print(ext_alert)


    def local_run(self, alert_json_path: str, write_to_s3=False, verbose=True, dry_run=True, alertname=None):
        with open(alert_json_path, 'r') as f:
            record = f.read()
            alert, ext_alert = self._listen(alert=record, write_to_s3=write_to_s3, verbose=verbose, dry_run=dry_run, alertname=alertname)
            if verbose:
                print(alert)
                if ext_alert:
                    print()
                    print(ext_alert)


if __name__ == '__main__':
    atype = "LIGO_ALERT"
    l = Listener(listener_type=atype)
    l.run(write_to_s3=False, verbose=True, dry_run=True)
