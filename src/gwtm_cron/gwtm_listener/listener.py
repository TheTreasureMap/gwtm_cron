import signal
import sys
import os

from gcn_kafka import Consumer # type: ignore

try:
    from . import gw_config as config
    from . import ligo_alert
    from . import icecube_notice
    from . import logger as log_module
except ImportError:
    # If running as a script, import from the parent directory
    import gw_config as config # type: ignore
    import ligo_alert # type: ignore
    import icecube_notice # type: ignore
    import logger as log_module # type: ignore

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

    def __init__(self, listener_type, config_path: str = "home/azureuser/cron/listener_config.json"):

        assert listener_type in LISTENER_TYPES.keys(), "Invalid Listener Type"

        self.listener_type = listener_type
        self.running = True

        # Initialize logger
        # Use JSON logging if LOG_FORMAT=json, otherwise use print statements
        self.use_json_logging = os.environ.get('LOG_FORMAT', '').lower() == 'json'
        if self.use_json_logging:
            self.logger = log_module.get_logger(__name__)

        self.config = config.Config(path_to_config=config_path)

        self.consumer = Consumer(
            client_id=self.config.KAFKA_CLIENT_ID,
            client_secret=self.config.KAFKA_CLIENT_SECRET
        )

        self.consumer.subscribe([
            LISTENER_TYPES[self.listener_type]["domain"]
        ])

        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGTERM, self._shutdown_handler)
        signal.signal(signal.SIGINT, self._shutdown_handler)

    def _log(self, message: str, level: str = 'info', **kwargs):
        """Log message using JSON or print depending on configuration"""
        if self.use_json_logging:
            log_func = getattr(self.logger, level)
            log_func(message, extra={'listener_type': self.listener_type, **kwargs})
        else:
            print(message)

    def _shutdown_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        self._log(f"Received signal {signum}, initiating graceful shutdown...", level='warning')
        self.running = False


    def _listen(self, alert, write_to_storage, verbose, dry_run, alertname=None):
        listener_function = LISTENER_TYPES[self.listener_type]["func"]
        return listener_function(self.config, alert, write_to_storage, verbose, dry_run, alertname)


    def run(self, write_to_storage=True, verbose=False, dry_run=False):
        if verbose:
            self._log(f'Listening for alerts from {LISTENER_TYPES[self.listener_type]["domain"]}',
                     domain=LISTENER_TYPES[self.listener_type]["domain"])

        while self.running:
            for message in self.consumer.consume(timeout=1):
                if not self.running:
                    break

                alert, ext_alert = self._listen(
                    alert=message.value(),
                    write_to_storage=write_to_storage,
                    verbose=verbose,
                    dry_run=dry_run
                )
                if verbose:
                    if self.use_json_logging:
                        self._log("Alert processed", alert_summary=str(alert)[:200])
                        if ext_alert:
                            self._log("Extended alert processed", ext_alert_summary=str(ext_alert)[:200])
                    else:
                        print(alert)
                        if ext_alert:
                            print()
                            print(ext_alert)

        # Cleanup on shutdown
        if verbose:
            self._log("Closing Kafka consumer...")
        self.consumer.close()
        if verbose:
            self._log("Listener shutdown complete")


    def local_run(self, alert_json_path: str, write_to_storage=False, verbose=True, dry_run=True, alertname=None):
        with open(alert_json_path, 'r') as f:
            record = f.read()
            alert, ext_alert = self._listen(alert=record, write_to_storage=write_to_storage, verbose=verbose, dry_run=dry_run, alertname=alertname)
            if verbose:
                print(alert)
                if ext_alert:
                    print()
                    print(ext_alert)


if __name__ == '__main__':
    atype = "LIGO_ALERT"
    l = Listener(listener_type=atype)  # noqa: E741
    l.run(write_to_storage=False, verbose=True, dry_run=True)
