import json
import datetime
import requests


from base64 import b64decode
from io import BytesIO
from astropy.table import Table
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


from find_galaxies import EventLocalization,generate_galaxy_list


def listen(config : config, alert, write_to_s3=True, verbose=False, dry_run=False, alertname=None):

    record = json.loads(alert)

    run_test = True
    if record['superevent_id'][0:2] != 'MS':
        run_test = False

    s3path = 'test' if run_test else 'fit'
    alert_role = 'test' if run_test else 'observation'

    writer = io.Writer(
        alert=alert,
        s3path=s3path,
        write_to_s3=write_to_s3
    )

    gwa = {}
    ext_gwa = None

    alert_keys = record.keys()
    gwa.update({
            "datecreated"       : datetime.datetime.now().strftime('"%Y-%m-%dT%H:%M:%S.%f"'),
            "observing_run"     : config.OBSERVING_RUN,
            "description"       : "O4 Alert",
            "role"              : alert_role,
            "graceid"           : record["superevent_id"] if "superevent_id" in alert_keys else 'ERROR',
            "alert_type"        : record["alert_type"] if "alert_type" in alert_keys else 'ERROR',
    })

    gwa["alert_type"], gwa["packet_type"] = function.get_packet_type(gwa["alert_type"])

    if alertname is None:
        path_info = gwa["graceid"] + '-' + gwa["alert_type"]

        alertinfo = function.query_gwtm_alerts(gwa['graceid'], gwa['alert_type'], config=config)

        if len(alertinfo) > 0:
            path_info = path_info + str(len(alertinfo))
    else:
        path_info = alertname

    writer.set_path_info(path_info=path_info)
    writer.write_alert_json(config, verbose=verbose)

    if "event" in alert_keys and isinstance(record["event"], dict):
        event_keys = record["event"].keys()
        record_event = record["event"]
        gwa.update({
            "far"               : record_event["far"] if "far" in event_keys else 0.0,
            "group"             : record_event["group"] if "group" in event_keys else "",
            "pipeline"          : record_event["pipeline"] if "pipeline" in event_keys else "",
            "search"            : record_event["search"] if "search" in event_keys else "",
            "centralfreq"      : record_event["central_frequency"] if "central_frequency" in event_keys else 0.0,
            "duration"          : record_event["duration"] if "duration" in event_keys else 0.0
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

            ra, dec = function.get_skymap_avg_pos(skymap)
            area_90, area_50 = function.get_skymap_90_50_area(skymap)

            header = skymap.meta
            header_keys = header.keys()

            #This is dumb
            skymap_url = None
            map_files = ["cWB.fits.gz", "bilby.fits.gz", "bayestar.fits.gz"]
            for mf in map_files:
                mf_url = f"https://gracedb.ligo.org/api/superevents/{gwa['graceid']}/files/{mf}"
                mf_r = requests.head(mf_url)
                if mf_r.status_code == 200:
                    skymap_url = mf_url
                    break
            if skymap_url is None:
                skymap_url = "Invalid.Sky.Map.URL"

            gwa.update({
                "skymap_fits_url" : skymap_url,
                "avgra"           : ra.deg,
                "avgdec"          : dec.deg,
                "area_90"         : area_90.to_value(u.deg**2),
                "area_50"         : area_50.to_value(u.deg**2),
                "time_of_signal"  : header['DATE-OBS'] if 'DATE-OBS' in header_keys else '1991-12-23T19:15:00',
                "distance"        : header['DISTMEAN'] if 'DISTMEAN' in header_keys else "-999.9",
                "distance_error"  : header['DISTSTD'] if 'DISTSTD' in header_keys else "-999.9",
                "timesent"        : header['DATE'] if 'DATE' in header_keys else '1991-12-23T19:15:00',
            })
            
            # create EventLocatlization object to be passed into the galaxies list
            gwa_obj = EventLocalization(gwa)

            #makes galaxy list, posts to API
            find_galaxies(gwa_obj)

            writer.set_gwalert_dict(gwa)
            writer.set_skymap(skymap_bytes)
            writer.process(config=config, verbose=verbose)
        
    if "external_coinc" in alert_keys and record["external_coinc"] is not None:
        ext_coin = record["external_coinc"]
        ext_coin_keys = ext_coin.keys()
        ext_gwa = gwa.copy()

        ext_gwa.update({
                "alert_type"        : f"{gwa['alert_type']}-ExtCoinc",
        })

        ext_gwa["alert_type"], ext_gwa["packet_type"] = function.get_packet_type(ext_gwa["alert_type"])

        ext_path_info = ext_gwa["graceid"] + '-' + ext_gwa["alert_type"]

        ext_alertinfo = function.query_gwtm_alerts(ext_gwa['graceid'], ext_gwa['alert_type'], config=config)

        if len(ext_alertinfo) > 0:
            ext_path_info = ext_path_info + str(len(ext_alertinfo))

        writer.set_path_info(path_info=ext_path_info)

        ext_gwa.update({
            "gcn_notice_id"       : ext_coin["gcn_notice_id"] if "gcn_notice_id" in ext_coin_keys else -999,
            "ivorn"               : ext_coin["ivorn"] if "ivorn" in ext_coin_keys else "",
            "ext_coinc_observatory"         : ext_coin["observatory"] if "observatory" in ext_coin_keys else "",
            "ext_coinc_search"              : ext_coin["search"] if "search" in ext_coin_keys else "",
            "time_difference"                   : ext_coin["time_difference"] if "time_difference" in ext_coin_keys else -999.9,
            "time_coincidence_far"              : ext_coin["time_coincidence_far"] if "time_coincidence_far" in ext_coin_keys else -999.9,
            "time_sky_position_coincidence_far" : ext_coin["time_sky_position_coincidence_far"] if "time_sky_position_coincidence_far" in ext_coin_keys else -999.9
        })

        if "combined_skymap" in ext_coin_keys:
            
            combined_skymap_str = ext_coin["combined_skymap"]
            combined_skymap_bytes = b64decode(combined_skymap_str)
            combined_skymap = Table.read(BytesIO(combined_skymap_bytes))

            ext_ra, ext_dec = function.get_skymap_avg_pos(combined_skymap)
            ext_area_90, ext_area_50 = function.get_skymap_90_50_area(combined_skymap)

            combined_header = combined_skymap.meta
            comb_header_keys = combined_header.keys()
            ext_gwa.update({
                "avgra"           : ext_ra.deg,
                "avgdec"          : ext_dec.deg,
                "area_90"         : ext_area_90.to_value(u.deg**2),
                "area_50"         : ext_area_50.to_value(u.deg**2),
                "time_of_signal"  : combined_header['DATE-OBS'] if 'DATE-OBS' in comb_header_keys else '1991-12-23T19:15:00',
                "distance"        : combined_header['DISTMEAN'] if 'DISTMEAN' in comb_header_keys else "-999.9",
                "distance_error"  : combined_header['DISTSTD'] if 'DISTSTD' in comb_header_keys else "-999.9",
                "timesent"        : combined_header['DATE'] if 'DATE' in comb_header_keys else '1991-12-23T19:15:00',
            })

            writer.set_gwalert_dict(ext_gwa)
            writer.set_skymap(combined_skymap_bytes)
            writer.process_external_coinc(config=config, verbose=verbose)

    if not dry_run:
        gwa = function.post_gwtm_alert(gwa, config=config)
        if ext_gwa is not None:
            ext_gwa = function.post_gwtm_alert(ext_gwa, config=config)
    
    if run_test:
        function.del_test_alerts(config=config)

    return gwa, ext_gwa

if __name__ == '__main__':
    l = listener.Listener(listener_type="LIGO_ALERT")
    l.run(write_to_s3=True, verbose=True, dry_run=False)
