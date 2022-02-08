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

DATA_PATH = "/Users/brendan/Documents/mpa_project/gfw_research/data/"

def DAILY_MMSI_FOLDER(year_str):
    return  f'mmsi_daily_csvs_v2/mmsi-daily-csvs-10-v2-{year_str}/'

POINTS_FOLDER = "points/"

MPAS_FOLDER = "mpas/"

PROTECTED_PLANET_MPA_FILENAMES = [
    # I renamed the 3 folders these came in, but the file names are how I downloaded them.
    "mpas/0/WDPA_WDOECM_Jan2022_Public_marine_shp-polygons.shp",
    "mpas/1/WDPA_WDOECM_Jan2022_Public_marine_shp-polygons.shp",
    "mpas/2/WDPA_WDOECM_Jan2022_Public_marine_shp-polygons.shp",
]

MPATLAS_FILENAME = "mpatlas_20201223_clean/mpatlas_20201223_clean.shp"

def load_mpatlas_mpas():
    mpas = geopandas.read_file(DATA_PATH + MPAS_FOLDER + MPATLAS_FILENAME)
    mpas.to_parquet(DATA_PATH + MPAS_FOLDER + "mpatlas.parquet")
    return mpas




def load_protected_planet_mpas():
    """
    This won't work anymore because I moved things around in their folders
    but serves as an example for what I did (which is just geopandas.read_file([.shp file])
    """
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

def convert_mpas():
    mpas = load_mpas()
    mpas.to_parquet(DATA_PATH + MPAS_FOLDER + "mpas.parquet")


def load_points(year_str):

    ## TODO - what about filtering out points with no fishing hours?

    FOLDER = DAILY_MMSI_FOLDER(year_str)
    ## Load the fishing hours data (this is kinda slow)
    filenames = os.listdir(DATA_PATH + FOLDER)# 'mmsi_daily_csvs_v2')

    # this might be faster but the status printout is nice:
    # points = pandas.concat([geopandas.read_file('data/daily_csvs/' + filename) for filename in filenames])


    count = 1
    points = []
    for filename in filenames:
        print(f'\r {filename} {count}/{len(filenames)}', end='')
        points.append(pandas.read_csv(DATA_PATH + FOLDER + filename,
            usecols=['date', 'cell_ll_lat', 'cell_ll_lon', 'mmsi', 'fishing_hours'],
            # used to supply dtype={} dictionary but doesn't seem necessary anymore
            parse_dates=['date']))
        count += 1
    print(' done.\n', end='')
    points = pandas.concat(points)  # deliberately overwriting
    points = points[points['fishing_hours'] > 0] # remove points without fishing hours
    return points

def convert_points_to_parquet(year_str):
    # filenames = os.listdir(DATA_PATH + 'daily_csvs_v2')
    # counted = 0
    # points = []
    # for filename in filenames:
    #     if year in filename:
    #         print(f'\r {filename} {counted}/{len(filenames)}', end='')
    #         points.append(pandas.read_csv(DATA_PATH + 'daily_csvs_v2/' + filename,
    #                                           dtype={'lat_bin': 'int16',
    #                                                  'lon_bin': 'int16',
    #                                                  'mmsi': 'int32',
    #                                                  'fishing_hours': 'float32'},
    #                                      parse_dates=['date']))
    #         counted += 1
    points = load_points(year_str)
    points.to_parquet(DATA_PATH + POINTS_FOLDER + year_str + ".parquet")


def convert_all():
    for year in range(2012, 2021):
        year_str = str(year)
        print("converting " + year_str)
        convert_points_to_parquet(year_str)
