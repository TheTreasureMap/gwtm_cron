import datetime
import os
import json
import ligo.skymap  # type: ignore
import ligo.skymap.io  # type: ignore
import ligo.skymap.postprocess  # type: ignore
import tempfile
import requests  # type: ignore

from ligo.skymap.healpix_tree import interpolate_nested  # type: ignore
from astropy.coordinates import SkyCoord  # type: ignore
from mocpy import MOC  # type: ignore

import numpy as np


try:
    from . import gw_function as function
    from . import gw_config as config
    from . import gwstorage
except ImportError:
    # If running as a script, import from the parent directory
    import gw_function as function  # type: ignore
    import gw_config as config  # type: ignore
    import gwstorage  # type: ignore

class Writer():

    def __init__(
            self,
            alert,
            s3path: str,
            write_to_storage = True
        ):

        self.write_to_storage = write_to_storage
        self.s3path = s3path

        if isinstance(alert, bytes):
            alert = alert.decode('utf-8')
        self.alert = alert

        self.skymap = None
        self.path_info = None
        self.gwalert_dict: dict = {}

        if not write_to_storage:
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
        self._write_skymap_moc(config=config, verbose=verbose)
        self._write_contours(config=config, verbose=verbose)
        self._write_fermi(config=config, verbose=verbose)
        self._write_LAT(config=config, verbose=verbose)


    def process_external_coinc(self, config: config.Config, verbose=False):
        self._write_skymap(config=config, verbose=verbose)
        self._write_skymap_moc(config=config, verbose=verbose)
        self._write_contours(config=config, verbose=verbose)


    def _write_skymap(self, config: config.Config, verbose=False):
        if verbose:
            print('Downloading skymap_fits_url')

        try:
            r = requests.get(self.gwalert_dict['skymap_fits_url'])
        except Exception:
            print(f"Bad skymap URL! {self.gwalert_dict['skymap_fits_url']} Gracedb might be bogged")
            return
        
        if self.write_to_storage:
            if verbose:
                print('Writing skymap.fits.gz to storage')
            downloadpath = '{}/{}.fits.gz'.format(self.s3path, self.path_info)
            gwstorage.upload_gwtm_file(r.content, downloadpath, source=config.STORAGE_BUCKET_SOURCE, config=config)
        else:
            if verbose:
                print('Writing skymap.fits.gz to local')
            local_write_path = os.path.join(os.getcwd(), 'skymaps', f"{self.path_info}.fits.gz")
            with open(local_write_path, 'wb') as f:
                f.write(r.content)


    def _write_skymap_moc(self, config: config.Config, verbose=False):

        if self.write_to_storage:
            if verbose:
                print('Writing skymap_moc.fits.gz to storage')
            downloadpath = '{}/{}_moc.fits.gz'.format(self.s3path, self.path_info)
            gwstorage.upload_gwtm_file(self.skymap, downloadpath, source=config.STORAGE_BUCKET_SOURCE, config=config)
        else:
            if verbose:
                print('Writing skymap_moc.fits.gz to local')
            local_write_path = os.path.join(os.getcwd(), 'skymaps', f"{self.path_info}_moc.fits.gz")
            with open(local_write_path, 'wb') as f:
                f.write(self.skymap) #type: ignore


    def _write_contours(self, config: config.Config, verbose=False):

        if verbose:
            print('Calculating 90/50 contours')

        with tempfile.NamedTemporaryFile(suffix='.fits.gz', delete=False) as tmp:
            tmp.write(self.skymap)  #type: ignore
            tmp_path = tmp.name
        try:
            pixel_prob, _ = ligo.skymap.io.fits.read_sky_map(tmp_path, nest=None)
        finally:
            os.unlink(tmp_path)
        pixel_prob = interpolate_nested(pixel_prob, nest=True)
        sorted_indices = np.flipud(np.argsort(pixel_prob))
        cumulative_prob = np.cumsum(pixel_prob[sorted_indices])
        credible_level = np.empty_like(pixel_prob)
        del pixel_prob
        credible_level[sorted_indices] = cumulative_prob * 100
        del sorted_indices, cumulative_prob
        contour_paths = list(ligo.skymap.postprocess.contour(credible_level, [50, 90], nest=True, degrees=True, simplify=True))
        del credible_level

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
                for contour, path in zip([50, 90], contour_paths)
            ]
        })

        if self.write_to_storage:
            if verbose:
                print('Writing contours to storage')

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

        try:
            tos = datetime.datetime.fromisoformat(self.gwalert_dict["time_of_signal"])
            earth_ra,earth_dec,earth_rad=function.getearthsatpos(tos)
            contour = function.makeEarthContour(earth_ra,earth_dec,earth_rad)
            skycoord = SkyCoord(contour, unit="deg", frame="icrs")

            moc = MOC.from_polygon_skycoord(skycoord, max_depth=9)
            moc = moc.complement()
            mocfootprint = moc.serialize(format='json')
            moc_string = json.dumps(mocfootprint)
        except Exception:
            print("Error in Fermi MOC creation")
            return

        if self.write_to_storage:
            if verbose:
                print('Writing Fermi contour to storage')
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

        tos = datetime.datetime.fromisoformat(self.gwalert_dict["time_of_signal"])
        try:
            ra, dec = function.getFermiPointing(tos)
            pointing_footprint= function.makeLATFoV(ra,dec)
            skycoord = SkyCoord(pointing_footprint, unit="deg", frame="icrs")
            moc = MOC.from_polygon_skycoord(skycoord, max_depth=9)
            mocfootprint = moc.serialize(format='json')
            moc_string = json.dumps(mocfootprint)
        except Exception:
            print("Error in LAT creation")
            return

        if self.write_to_storage:
            if verbose:
                print('Writing LAT contour to storage')

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
            function that writes the alert json to storage or local directory
        '''


        if self.write_to_storage:
            if verbose:
                print("Writing alert json to storage")

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

    def __init__(self, read_from_storage=True):
        self.read_from_storage = read_from_storage

    def read_alert_json(self, alert_path_name, config: config.Config, verbose=False):
        '''
            function that reads the alert json from storage or local directory
        '''
        if self.read_from_storage:
            if verbose:
                print('Reading alert json from storage')
            try:
                f = gwstorage.download_gwtm_file(alert_path_name, config.STORAGE_BUCKET_SOURCE, config)
                data = json.loads(f)
                return data
            except Exception:
                print('Error in s3 download, file might not exist')
