import json
import datetime

import numpy as np
import astropy_healpix as ah

from base64 import b64decode
from io import BytesIO
from astropy.table import Table
from gcn_kafka import Consumer

from . import config
from . import function
from . import io

class Listener():

    def __init__(self):
        self.config = config.Config()
        self.consumer = Consumer(
            client_id=self.config.KAFKA_CLIENT_ID,
            client_secret=self.config.KAFKA_CLIENT_SECRET
        )
        
        self.consumer.subscribe([
            self.config.ALERT_DOMAIN
        ])


    def _listen(self, alert, write_to_s3=True, verbose=False, dry_run=False):

        record = json.loads(alert)

        run_test = True
        if record['superevent_id'][0] != 'M':
            run_test = False

        s3path = 'test' if run_test else 'fit'
        alert_role = 'test' if run_test else 'observation'

        writer = io.Writer(
            alert=alert,
            s3path=s3path,
            write_to_s3=write_to_s3
        )

        gwa = {}

        alert_keys = record.keys()
        gwa.update({
                "datecreated"       : datetime.datetime.now().strftime('"%Y-%m-%dT%H:%M:%S.%f"'),
                "observing_run"     : self.config.OBSERVING_RUN,
                "description"       : "O4 Alert",
                "role"              : alert_role,
                "graceid"           : record["superevent_id"] if "superevent_id" in alert_keys else 'ERROR',
                "alert_type"        : record["alert_type"] if "alert_type" in alert_keys else 'ERROR',
        })

        gwa["alert_type"], gwa["packet_type"] = function.get_packet_type(gwa["alert_type"])

        path_info = gwa["graceid"] + '-' + gwa["alert_type"]

        alertinfo = function.query_gwtm_alerts(gwa['graceid'], gwa['alert_type'], config=self.config)

        if len(alertinfo) > 0:
            path_info = path_info + str(len(alertinfo))

        writer.set_path_info(path_info=path_info)
        writer.write_alert_json(self.config, verbose=verbose)

        if "event" in alert_keys and isinstance(record["event"], dict):
            event_keys = record["event"].keys()
            record_event = record["event"]
            gwa.update({
                "far"               : record_event["far"] if "far" in event_keys else 0.0,
                "group"             : record_event["group"] if "group" in event_keys else "",
                "pipeline"          : record_event["pipeline"] if "pipeline" in event_keys else "",
                "search"            : record_event["search"] if "search" in event_keys else "",
            })

            if "instruments" in event_keys:
                if isinstance(record_event["instruments"], list):
                    gwa.update({
                        "detectors" : ','.join(record_event["instruments"])
                    })
                else:
                    gwa.update({
                        "detectors" : record_event["instruments"]
                    })
            
            if "properties" in event_keys and isinstance(record_event["properties"], dict):
                record_event_prop = record_event["properties"]
                property_keys = record_event_prop.keys()
                gwa.update({
                    "prob_hasns"       : record_event_prop["HasNS"] if "HasNS" in property_keys else 0.0,
                    "prob_hasremenant" : record_event_prop["HasRemnant"] if "HasRemnant" in property_keys else 0.0,
                    "prob_gap"         : record_event_prop["HasMassGap"] if "HasMassGap" in property_keys else 0.0,
                })

            if "classification" in event_keys and isinstance(record_event["classification"], dict):
                record_event_class = record_event["classification"]
                class_keys = record_event_class.keys()
                gwa.update({
                    "prob_bns"         : record_event_class["BNS"] if "BNS" in class_keys else 0.0,
                    "prob_nsbh"        : record_event_class["NSBH"] if "NSBH" in class_keys else 0.0,
                    "prob_bbh"         : record_event_class["BBH"] if "BBH" in class_keys else 0.0,
                    "prob_terrestrial" : record_event_class["Terrestrial"] if "Terrestrial" in class_keys else 0.0

                })

            if "skymap" in event_keys:
                skymap_str = record_event["skymap"]
                skymap_bytes = b64decode(skymap_str)
                skymap = Table.read(BytesIO(skymap_bytes))

                level, ipix = ah.uniq_to_level_ipix(
                    skymap[np.argmax(skymap['PROBDENSITY'])]['UNIQ']
                )
                ra, dec = ah.healpix_to_lonlat(ipix, ah.level_to_nside(level), order='nested')

                header = skymap.meta
                header_keys = header.keys()
                gwa.update({
                    "skymap_fits_url" : "internally read",
                    "avgra"           : ra.deg,
                    "avgdec"          : dec.deg,
                    "time_of_signal"  : header['DATE-OBS'] if 'DATE-OBS' in header_keys else '1991-12-23T19:15:00',
                    "distance"        : header['DISTMEAN'] if 'DISTMEAN' in header_keys else "-999.9",
                    "distance_error"  : header['DISTSTD'] if 'DISTSTD' in header_keys else "-999.9",
                    "timesent"        : header['DATE'] if 'DATE' in header_keys else '1991-12-23T19:15:00',
                })

                writer.set_gwalert_dict(gwa)
                writer.set_skymap(skymap_bytes)
                writer.process(config=self.config, verbose=verbose)

        if not dry_run:
            gwa = function.post_gwtm_alert(gwa, config=self.config)

        return gwa


    def run(self, write_to_s3=True, verbose=False, dry_run=False):
        if verbose:
            print(f'Listening for alerts from {self.config.ALERT_DOMAIN}')

        while True:
            for message in self.consumer.consume(timeout=1):
                alert = self._listen(
                    alert=message.value(), 
                    write_to_s3=write_to_s3,
                    verbose=verbose,
                    dry_run=dry_run
                )
                if verbose:
                    print(alert)


    def local_run(self, alert_json_path: str, write_to_s3=False, verbose=True, dry_run=True):
        with open(alert_json_path, 'r') as f:
            record = f.read()
            alert = self._listen(alert=record, write_to_s3=write_to_s3, verbose=verbose, dry_run=dry_run)
            if verbose:
                print(alert)