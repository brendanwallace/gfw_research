import numpy as np
import pandas
from datetime import datetime
import matplotlib.pyplot as plt
import os
import pywdpa
import geopandas
import contextily as ctx
from shapely import geometry
import pretty_html_table


MPA_FILENAMES = [
    "../data/WDPA_WDOECM_wdpa_shp/WDPA_WDOECM_wdpa_shp0/WDPA_WDOECM_wdpa_shp-polygons.shp",
    "../data/WDPA_WDOECM_wdpa_shp/WDPA_WDOECM_wdpa_shp1/WDPA_WDOECM_wdpa_shp-polygons.shp",
    "../data/WDPA_WDOECM_wdpa_shp/WDPA_WDOECM_wdpa_shp2/WDPA_WDOECM_wdpa_shp-polygons.shp",
]

def load_mpas(verbose=True, filenames=MPA_FILENAMES):
    protected_areas = []
    counted = 0
    for filename in filenames:
        if verbose:
            print(f'\rloading mpas: {counted}/{len(filenames)}', end='')
        protected_areas.append(geopandas.read_file(filename))
        counted += 1
    if verbose:
        print(f'\rloading mpas: {counted}/{len(filenames)} done.', end='')

        
    protected_areas = pandas.concat(protected_areas)
    # filters for marine only (may want to change this to 1 or 2 (2 is marine only, 1 is mixed, 0 is terrestrial))
    return protected_areas[protected_areas["MARINE"] == "2"]

def load_points(verbose=True):
    pass

load_mpas()