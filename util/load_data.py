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

DATA_PATH = "/Users/brendan/Masters/gfw_research/data/"

MPA_FILENAMES = [
    "WDPA_WDOECM_wdpa_shp/WDPA_WDOECM_wdpa_shp0/WDPA_WDOECM_wdpa_shp-polygons.shp",
    "WDPA_WDOECM_wdpa_shp/WDPA_WDOECM_wdpa_shp1/WDPA_WDOECM_wdpa_shp-polygons.shp",
    "WDPA_WDOECM_wdpa_shp/WDPA_WDOECM_wdpa_shp2/WDPA_WDOECM_wdpa_shp-polygons.shp",
]


def load_mpas():
    # reads the downloaded WPDA polygon files
    protected_areas = []
    counted = 0
    for filename in MPA_FILENAMES:
        print(f'\rloading mpas: {counted}/{len(MPA_FILENAMES)}', end='')
        protected_areas.append(geopandas.read_file(DATA_PATH + filename))
        counted += 1
    print(f'\rloading mpas: {counted}/{len(MPA_FILENAMES)} done.')


    protected_areas = pandas.concat(protected_areas)
    # filters for marine only (may want to change this to 1 or 2 (2 is marine only, 1 is mixed, 0 is terrestrial))
    mpas = protected_areas[protected_areas["MARINE"] == "2"]
    return mpas


def load_points():
    ## Load the fishing hours data (this is kinda slow)
    print('loading points')
    filenames = os.listdir(DATA_PATH + 'daily_csvs_v2')

    # this might be faster but the status printout is nice:
    # points = pandas.concat([geopandas.read_file('data/daily_csvs/' + filename) for filename in filenames])

    counted = 0
    points = []
    for filename in filenames:
        print(f'\r {filename} {counted}/{len(filenames)}', end='')
        points.append(pandas.read_csv(DATA_PATH + 'daily_csvs_v2/' + filename,
                                          dtype={'lat_bin': 'int16',
                                                 'lon_bin': 'int16',
                                                 'mmsi': 'int32',
                                                 'fishing_hours': 'float32'},
                                     parse_dates=['date']))
        counted += 1
    print('\nloaded; concatenating.')
    return pandas.concat(points) # deliberately overwriting points

def convert_points_to_parquet(year):
    filenames = os.listdir(DATA_PATH + 'daily_csvs_v2')
    counted = 0
    points = []
    for filename in filenames:
        if year in filename:
            print(f'\r {filename} {counted}/{len(filenames)}', end='')
            points.append(pandas.read_csv(DATA_PATH + 'daily_csvs_v2/' + filename,
                                              dtype={'lat_bin': 'int16',
                                                     'lon_bin': 'int16',
                                                     'mmsi': 'int32',
                                                     'fishing_hours': 'float32'},
                                         parse_dates=['date']))
            counted += 1
    points = pandas.concat(points)
    points.to_parquet(year + ".parquet")


def convert_all():
    for year in range(2012, 2021):
        year_str = str(year)
        print("converting " + year_str)
        convert_points_to_parquet(year_str)
