import json
import datetime
import requests

import numpy as np
import astropy_healpix as ah

from base64 import b64decode
from io import BytesIO
from astropy.table import Table
from gcn_kafka import Consumer
from astropy import units as u

try:
    from . import listener
    from . import gw_config as config
    from . import gw_function as function
    from . import gw_io as io
except:
    import listener
    import gw_config as config
    import gw_function as function
    import gw_io as io


def listen(config, alert, write_to_s3=True, verbose=False, dry_run=False, alertname=None):

    record = json.loads(alert)
    rkeys = record.keys()

    icecube_notice = {
        "graceid" : record["ref_ID"] if "ref_ID" in rkeys else "error",
        "alert_datetime" : record["alert_datetime"] if "alert_datetime" in rkeys else '1991-12-23T19:15:00',
        "observation_start" : record["observation_start"] if "observation_start" in rkeys else '1991-12-23T19:15:00',
        "observation_stop" : record["observation_stop"] if "observation_stop" in rkeys else '1991-12-23T19:15:00',
        "pval_generic" : record["pval_generic"] if "pval_generic" in rkeys else '0.0',
        "pval_bayesian" : record["pval_bayesian"] if "pval_bayesian" in rkeys else '0.0',
    }

    if "most_probable_direction" in rkeys:
        mpd = record["most_probable_direction"]
        icecube_notice.update({
            "most_probable_direction_ra" : mpd["ra"] if "ra" in mpd.keys() else -999,
            "most_probable_direction_dec" : mpd["dec"] if "dec" in mpd.keys() else -999
        })

    if "neutrino_flux_sensitivity_range" in rkeys:
        nfsr = record["neutrino_flux_sensitivity_range"]
        if "flux_sensitivity" in nfsr.keys() and isinstance(nfsr["flux_sensitivity"], list):
            if len(nfsr["flux_sensitivity"]) == 2:
                icecube_notice.update({
                    "flux_sens_low" : nfsr["flux_sensitivity"][0],
                    "flux_sens_high" : nfsr["flux_sensitivity"][1]
                })

        if "sensitive_energy_range" in nfsr.keys() and isinstance(nfsr["sensitive_energy_range"], list):
            if len(nfsr["sensitive_energy_range"]) == 2:
                icecube_notice.update({
                    "sens_energy_range_low" : nfsr["sensitive_energy_range"][0],
                    "sens_energy_range_high" : nfsr["sensitive_energy_range"][1]
                })
        

    if "coincident_events" in rkeys:
        cevents = record["coincident_events"]
        icecube_coincident_events = []

        for event in cevents:
            ekeys = event.keys()
            event_record = {
                "event_dt" : event["event_dt"] if "event_dt" in ekeys else -999,
                "ra" : event["ra"] if "ra" in ekeys else -999,
                "dec" : event["dec"] if "dec" in ekeys else -999,
                "containment_probability" : event["containment_probability"] if "containment_probability" in ekeys else 0.0,
                "event_pval_generic" : event["event_pval_generic"] if "event_pval_generic" in ekeys else 0.0,
                "event_pval_bayesian" : event["event_pval_bayesian"] if "event_pval_bayesian" in ekeys else 0.0,
            }
            if "ra_uncertainty" in ekeys:
                if isinstance(event["ra_uncertainty"], list):
                    event_record.update({
                        "ra_uncertainty" : event["ra_uncertainty"][0]
                    })
                else:
                    event_record.update({
                        "ra_uncertainty" : event["ra_uncertainty"]
                    })
            
            icecube_coincident_events.append(event_record)

    return icecube_notice, icecube_coincident_events

if __name__ == '__main__':
    l = listener.Listener(listener_type="ICECUBE_NOTICE")
    l.run(write_to_s3=True, verbose=True, dry_run=False)