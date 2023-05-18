import io
import datetime
import os
import json
import ligo.skymap
import ligo.skymap.io
import ligo.skymap.postprocess
import tempfile
from . import gwstorage

from ligo.skymap.healpix_tree import interpolate_nested
from astropy.coordinates import SkyCoord
from mocpy import MOC

import numpy as np

try:
    from . import gw_function as function
    from . import gw_config as config
except:
    import gw_function as function
    import gw_config as config

class Writer():
    
    def __init__(
            self,
            alert,
            s3path: str,
            write_to_s3 = True
        ):

        self.write_to_s3 = write_to_s3
        self.s3path = s3path

        if isinstance(alert, bytes):
            alert = alert.decode('utf-8')
        self.alert = alert

        self.skymap = None
        self.path_info = None
        self.gwalert_dict = None

        if not write_to_s3:
            paths = [
                'saved_alerts',
                'skymaps',
                'contours'
            ]
            for p in paths:
                pway = os.path.join(os.getcwd(), p)
                if not os.path.exists(pway):
                    os.makedirs(pway, exist_ok=True)


    def set_skymap(self, skymap):
        self.skymap = skymap


    def set_path_info(self, path_info):
        self.path_info = path_info
    

    def set_gwalert_dict(self, gwalert_dict):
        self.gwalert_dict = gwalert_dict


    def process(self, config: config.Config, verbose=False):
        self._write_skymap(config=config, verbose=verbose)
        self._write_contours(config=config, verbose=verbose)
        self._write_fermi(config=config, verbose=verbose)
        self._write_LAT(config=config, verbose=verbose)


    def _write_skymap(self, config: config.Config, verbose=False):

        if self.write_to_s3:
            if verbose:
                print('Writing skymap.fits.gz to s3')
            downloadpath = '{}/{}.fits.gz'.format(self.s3path, self.path_info)
            gwstorage.upload_gwtm_file(self.skymap, downloadpath, source=config.STORAGE_BUCKET_SOURCE, config=config)
        else:
            if verbose:
                print('Writing skymap.fits.gz to local')
            local_write_path = os.path.join(os.getcwd(), 'skymaps', f"{self.path_info}.fits.gz")
            with open(local_write_path, 'wb') as f:
                f.write(self.skymap)


    def _write_contours(self, config: config.Config, verbose=False):

        if verbose:
            print('Calculating 90/50 contours')

        tmp = tempfile.NamedTemporaryFile()
        with open(tmp.name, 'wb') as f:
            f.write(self.skymap)

        prob, _ = ligo.skymap.io.fits.read_sky_map(tmp.name, nest=None)
        prob = interpolate_nested(prob, nest=True)
        i = np.flipud(np.argsort(prob))
        cumsum = np.cumsum(prob[i])
        cls = np.empty_like(prob)
        cls[i] = cumsum * 100
        paths = list(ligo.skymap.postprocess.contour(cls, [50, 90], nest=True, degrees=True, simplify=True))

        contours_json = json.dumps({
            'type': 'FeatureCollection',
            'features': [
                {
                    'type': 'Feature',
                    'properties': {
                        'credible_level': contour
                    },
                    'geometry': {
                        'type': 'MultiLineString',
                        'coordinates': path
                    }
                }
                for contour, path in zip([50,90], paths)
            ]
        })

        if self.write_to_s3:
            if verbose:
                print('Writing contours to s3')

            contour_download_path = '{}/{}-contours-smooth.json'.format(self.s3path, self.path_info)
            gwstorage.upload_gwtm_file(contours_json.encode(), contour_download_path, config.STORAGE_BUCKET_SOURCE, config)
        else:
            if verbose:
                print('Writing contours to local')

            local_write_path = os.path.join(os.getcwd(), 'contours', f"{self.path_info}-contours-smooth.json")
            with open(local_write_path, 'wb') as f:
                f.write(contours_json.encode())


    def _write_fermi(self, config: config.Config, verbose=False):
        if verbose:
            print('Calculating Fermi contour map')

        tos = datetime.datetime.strptime(self.gwalert_dict["time_of_signal"], "%Y-%m-%dT%H:%M:%S.%f")
        earth_ra,earth_dec,earth_rad=function.getearthsatpos(tos)
        contour = function.makeEarthContour(earth_ra,earth_dec,earth_rad)
        skycoord = SkyCoord(contour, unit="deg", frame="icrs")

        moc = MOC.from_polygon_skycoord(skycoord, max_depth=9)
        moc = moc.complement()
        mocfootprint = moc.serialize(format='json')
        moc_string = json.dumps(mocfootprint)

        if self.write_to_s3:
            if verbose:
                print('Writing Fermi contour to s3')
            fermi_moc_upload_path = '{}/{}-Fermi.json'.format(self.s3path, self.gwalert_dict["graceid"])
            dir_contents = gwstorage.list_gwtm_bucket(self.s3path, config.STORAGE_BUCKET_SOURCE, config)
            if fermi_moc_upload_path not in dir_contents:
                gwstorage.upload_gwtm_file(moc_string.encode(), fermi_moc_upload_path, config.STORAGE_BUCKET_SOURCE, config)

        else:
            local_write_file = os.path.join(os.getcwd(), "contours", f"{self.gwalert_dict['graceid']}-Fermi.json")
            if os.path.exists(local_write_file):
                if verbose:
                    print('Fermi File already exists')
                return
            
            if verbose:
                print('Writing Fermi contour to local')

            with open(local_write_file, "wb") as f:
                f.write(moc_string.encode())


    def _write_LAT(self, config: config.Config, verbose=False):
        
        if verbose:
            print('Calculating LAT contours')

        tos = datetime.datetime.strptime(self.gwalert_dict["time_of_signal"], "%Y-%m-%dT%H:%M:%S.%f")
        ra, dec = function.getFermiPointing(tos)
        pointing_footprint= function.makeLATFoV(ra,dec)
        skycoord = SkyCoord(pointing_footprint, unit="deg", frame="icrs")
        moc = MOC.from_polygon_skycoord(skycoord, max_depth=9)
        mocfootprint = moc.serialize(format='json')
        moc_string = json.dumps(mocfootprint)

        if self.write_to_s3:
            if verbose:
                print('Writing LAT contour to s3')

            lat_moc_upload_path = '{}/{}-LAT.json'.format(self.s3path, self.gwalert_dict["graceid"])
            dir_contents = gwstorage.list_gwtm_bucket(self.s3path, config.STORAGE_BUCKET_SOURCE, config)
            if lat_moc_upload_path not in dir_contents:
                gwstorage.upload_gwtm_file(moc_string.encode(), lat_moc_upload_path, config.STORAGE_BUCKET_SOURCE, config)
        
        else:
            local_write_file = os.path.join(os.getcwd(), "contours", f"{self.gwalert_dict['graceid']}-LAT.json")
            if os.path.exists(local_write_file):
                if verbose:
                    print('LAT File already exists')
                return
            
            if verbose:
                print('Writing LAT contour to local')

            with open(local_write_file, "wb") as f:
                f.write(moc_string.encode())


    def write_alert_json(self, config: config.Config, verbose=False):
        '''
            function that writes the alert json to s3 or local directory
        '''


        if self.write_to_s3:
            if verbose:
                print("Writing alert json to s3")

            alert_upload_path = os.path.join(self.s3path, f"{self.path_info}_alert.json")
            dir_contents = gwstorage.list_gwtm_bucket(self.s3path, config.STORAGE_BUCKET_SOURCE, config)
            if alert_upload_path not in dir_contents:
                gwstorage.upload_gwtm_file(self.alert.encode(), alert_upload_path, config.STORAGE_BUCKET_SOURCE, config)
        else:
            if verbose:
                print("Writing alert json to local")

            local_file_path = os.path.join(os.getcwd(), "saved_alerts", f"{self.path_info}_alert.json")
            with open(local_file_path, 'wb') as f:
                f.write(self.alert.encode())

class Reader():

    def __init__(self, read_from_s3=True):
        self.read_from_s3 = read_from_s3

    def read_alert_json(self, alert_path_name, config: config.Config, verbose=False):
        '''
            function that reads the alert json or local directory
        '''
        if self.read_from_s3:
            if verbose:
                print('Reading alert json from s3')
            try:
                f = gwstorage.download_gwtm_file(alert_path_name, config.STORAGE_BUCKET_SOURCE, config)
                data = json.loads(f)
                return data
            except:
                print('Error in s3 download, file might not exist')
