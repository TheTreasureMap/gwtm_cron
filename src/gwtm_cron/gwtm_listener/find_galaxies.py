import healpy as hp #  type: ignore[import]
import numpy as np
from astropy.table import Table #  type: ignore[import]
from configparser import ConfigParser
from scipy.stats import norm #  type: ignore[import]


#defines EventLocalization class to match formatting of tom toolkit model, input into generate_galaxy_list
#accepts dictionary from ligo_alert.py gwa dictionary
class EventLocalization(object):
    def __init__(self,gwa_dict):
        self.distance_mean = gwa_dict['distance']
        self.skymap_url = gwa_dict['skymap_fits_url']
        self.graceid = gwa_dict['graceid']
        self.timesent_stamp = gwa_dict['timesent']

    def __str__(self):
        return self.graceid



def generate_galaxy_list(eventlocalization: EventLocalization, galaxy_config_path: str, completeness=None, credzone=None, skymap_filepath=None):
    """
    An adaptation of the galaxy ranking algorithm described in
    Arcavi et al. 2017 (doi:10.3847/2041-8213/aa910f)
    
    eventlocalization: an EventLocalization object (is still true, no longer tom toolkit model)
    """

    # Parameters:
    try:
        galaxy_config = ConfigParser(inline_comment_prefixes=';')
        galaxy_config.read(galaxy_config_path)
        catalog_path = galaxy_config.get('GALAXIES', 'CATALOG_PATH') # Path to numpy file containing the galaxy catalog (faster than getting from the db)
    except Exception as e:
        print(e)
    # Matching parameters:
    if not credzone:
        credzone = float(galaxy_config.get('GALAXIES', 'CREDZONE')) # Localization probability to consider credible (e.g. 0.99)
    nsigmas_in_d = float(galaxy_config.get('GALAXIES', 'NSIGMAS_IN_D')) # Sigmas to consider in distnace (e.g. 3)
    if not completeness:
        completeness = float(galaxy_config.get('GALAXIES', 'COMPLETENESSP')) # Mass fraction completeness (e.g. 0.5)
    #minGalaxies = int(galaxy_config.get('GALAXIES', 'MINGALAXIES')) # Minimum number of galaxies to output (e.g. 100)
    
    minL = float(galaxy_config.get('GALAXIES', 'MINL')) # Estimated brightest KN luminosity
    maxL = float(galaxy_config.get('GALAXIES', 'MAXL')) # Estimated faintest KN luminosity
    sensitivity = float(galaxy_config.get('GALAXIES', 'SENSITIVITY')) # Estimatest faintest app mag we can see
    #ngalaxtoshow = int(galaxy_config.get('GALAXIES', 'NGALAXIES')) # Number of galaxies to show
    
    mindistFactor = float(galaxy_config.get('GALAXIES', 'MINDISTFACTOR')) #reflecting a small chance that the theory is comletely wrong and we can still see something
    
    ## Schecter Function parameters:
    #alpha = float(galaxy_config.get('GALAXIES', 'ALPHA'))
    #MB_star = float(galaxy_config.get('GALAXIES', 'MB_STAR'))
    
    try:
        if not eventlocalization.distance_mean:
            ### This is a burst alert, so just read the probabilities from the map
            ### and fix the distance to only look at nearby galaxies
            if skymap_filepath is not None:
                prob = hp.read_map(skymap_filepath, field=0)
            else:
                prob = hp.read_map(eventlocalization.skymap_url.replace('.multiorder.fits','.fits.gz'), field=0)
            ### Fix distance vectors:
            distmu = np.ones(len(prob)) * 10.0 # Fix to 10 Mpc
            distsigma = np.ones(len(prob)) * 10.0 # Fix to 10 Mpc
            distnorm = np.ones(len(prob)) # Flat prior?
        else:
            if skymap_filepath is not None:
                prob, distmu, distsigma, distnorm = hp.read_map(skymap_filepath, field=[0,1,2,3])
            else:
                prob, distmu, distsigma, distnorm = hp.read_map(eventlocalization.skymap_url.replace('.multiorder.fits','.fits.gz'), field=[0,1,2,3])

    except Exception as e:
        print('WARNING: Failed to read sky map for {}'.format(eventlocalization))
        print('WARNING:',e)
        return

    # Get the map parameters:
    npix = len(prob)
    nside = hp.npix2nside(npix)

    # Load the galaxy catalog.
    print('INFO: Loading Galaxy Catalog')
    galaxies = Table.read(catalog_path)

    ### If using luminosity, remove galaxies with no Lum_X, like so:q
    #galaxies = galaxies[~np.isnan(galaxies['Lum_W1'])]
    ### If using mass, make cuts on DistMpc and Mstar
    galaxies = galaxies[~np.isnan(galaxies['Mstar'])]
    galaxies = galaxies[np.where(galaxies['DistMpc']>0)] # Remove galaxies with distance < 0

    theta = 0.5 * np.pi - np.pi*(galaxies['dec'])/180
    phi = np.deg2rad(galaxies['ra'])
    d = np.array(galaxies['DistMpc'])
    # Convert galaxy coordinates to map pixels:
    print('INFO: Converting Galaxy Coordinates to Map Pixels')
    ipix = hp.ang2pix(nside, theta, phi)

    maxprobcoord_tup = hp.pix2ang(nside, np.argmax(prob))
    maxprobcoord = [0, 0]
    maxprobcoord[0] = np.rad2deg(0.5*np.pi-maxprobcoord_tup[0])
    maxprobcoord[1] = np.rad2deg(maxprobcoord_tup[1])
    
    #Find the zone with probability <= credzone:
    print('INFO: Finding zone with credible probability')
    probcutoff = 1
    probsum = 0

    sortedprob = np.sort(prob,kind="mergesort")
    while probsum < credzone:
        probsum = probsum + sortedprob[-1]
        probcutoff = sortedprob[-1]
        sortedprob = sortedprob[:-1]

    # Calculate the probability for galaxies according to the localization map:
    print('INFO: Calculating galaxy probabilities')
    p = prob[ipix]
    distp = (norm(distmu[ipix], distsigma[ipix]).pdf(d) * distnorm[ipix])

    # Cuttoffs: credzone of probability by angles and nsigmas by distance:
    inddistance = np.where(np.abs(d-distmu[ipix])<nsigmas_in_d*distsigma[ipix])
    indcredzone = np.where(p>=probcutoff)

    # Increase credzone to 99.995% if no galaxies found:
    # If no galaxies found in the credzone and within the right distance range
    if len(galaxies[np.intersect1d(indcredzone,inddistance)]) == 0:
        while probsum < 0.99995:
            if sortedprob.size == 0:
                break
            probsum = probsum + sortedprob[-1]
            probcutoff = sortedprob[-1]
            sortedprob = sortedprob[:-1]
        inddistance = np.where(np.abs(d - distmu[ipix]) < 5 * distsigma[ipix])
        indcredzone = np.where(p >= probcutoff)

    ipix = ipix[np.intersect1d(indcredzone, inddistance)]
    p = p[np.intersect1d(indcredzone, inddistance)]
    p = (p * (distp[np.intersect1d(indcredzone, inddistance)]))  ##d**2?

    galaxies = galaxies[np.intersect1d(indcredzone, inddistance)]
    if len(galaxies) == 0:
        print("WARNING: No galaxies found")
        print("WARNING: Peak is at [RA,DEC](deg) = {}".format(maxprobcoord))
        return

    ### Normalize by mass:
    ### NOTE: Can also do this in using luminosity

    mass = galaxies['Mstar']
    massNorm = mass / np.sum(mass)
    normalization = np.sum(p * massNorm)

    absolute_sensitivity = sensitivity - 5 * np.log10(galaxies['DistMpc'] * (10 ** 5))
    absolute_sensitivity = absolute_sensitivity.astype(np.float64)

    #absolute_sensitivity_lum = mag.f_nu_from_magAB(absolute_sensitivity)
    absolute_sensitivity_lum = 4e33 * 10**(0.4*(4.74-absolute_sensitivity)) # Check this?
    distanceFactor = np.zeros(len(galaxies))
    distanceFactor[:] = ((maxL - absolute_sensitivity_lum) / (maxL - minL))
    distanceFactor[mindistFactor>(maxL - absolute_sensitivity_lum) / (maxL - minL)] = mindistFactor
    distanceFactor[absolute_sensitivity_lum<minL] = 1
    distanceFactor[absolute_sensitivity>maxL] = mindistFactor

    # Sorting glaxies by probability
    ii = np.argsort(p*massNorm*distanceFactor,kind="mergesort")[::-1]

    ####counting galaxies that constitute 50% of the probability(~0.5*0.98)
    summ = 0
    galaxies50per = 0
    sum_seen = 0
    while summ<0.5:
        if galaxies50per>= len(ii):
            break
        summ = summ + (p[ii[galaxies50per]]*massNorm[ii[galaxies50per]])/float(normalization)
        sum_seen = sum_seen + (p[ii[galaxies50per]]*massNorm[ii[galaxies50per]]*distanceFactor[ii[galaxies50per]])/float(normalization)
        galaxies50per = galaxies50per+1

    #if want to limit by number of galaxies in .ini file

    # if len(ii) > ngalaxtoshow:
    #     n = ngalaxtoshow
    # else:
    #     n = len(ii)

    score=(p * massNorm / normalization)
    ra=galaxies['ra']
    dec=galaxies['dec']
    name = galaxies['objname']
    Mstar = galaxies['Mstar']
    dist = galaxies['DistMpc'].tolist()
    
    print('INFO: Finished creating ranked galaxy list for EventLocalization {}'.format(eventlocalization))

    iter = ii.tolist()
    galaxy_list = []
    for i in range(len(iter)):
        ind = iter[i]
        galaxy_list.append({
            "ra":ra[ind],
            "dec":dec[ind],
            "score":score[ind],
            "rank":i,
            "name":name[ind],
            "info":{
                'Mstar':Mstar[ind],
                'Distance [Mpc]':dist[ind]
            }
        })  
    
    post_galaxies_json = {
        "graceid":eventlocalization.graceid,
        "timesent_stamp":eventlocalization.timesent_stamp,
        "groupname":"LCOGT",
        "reference":"https://ui.adsabs.harvard.edu/abs/2017ApJ...848L..33A/abstract",
        "request_doi":True,
        "galaxies":galaxy_list
    }

    return post_galaxies_json