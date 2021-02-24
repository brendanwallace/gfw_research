import numpy as np
import pandas
from datetime import datetime
import matplotlib.pyplot as plt
import os
import pywdpa
import geopandas
import contextily as ctx
from shapely import geometry

def join_on_lat_lon(points):
    return points.groupby(['lat_bin', 'lon_bin'], as_index=False).aggregate({'lat_bin': 'first', 'lon_bin': 'first', 'fishing_hours': 'sum'})


## Compute geometries for the points
def compute_box(row, width=0.1):
    x = float(row['lon_bin'] * width)
    y = float(row['lat_bin'] * width)
    return geometry.box(x, y, x+width, y+width)


def convert_to_geo(points, box=True):
    if box:
        points['geometry'] = points.apply(compute_box, axis=1)
        return geopandas.GeoDataFrame(points, geometry=points['geometry']).set_crs(epsg=4326)
    # use points (not as precise, but cheaper)
    else:
        return geopandas.GeoDataFrame(
            points,
            geometry=geopandas.points_from_xy(points['lon_bin']*0.1 + 0.05, points['lat_bin']*0.1 + 0.05)
        ).set_crs(epsg=4326)


def table_of_in_out_pre_post(points_of_mpa_ships, mpa, date):
    """
    Returns a table with total fishing hours in/out of the mpa
    
    All it does is compute for each point whether it's in/out and before/after, and
    then aggregates the totals per combination for each mmsi (each unique ship).
    
    Tries to take advantage of table groupby and aggregation to do this.
    """
    geopoints_ = convert_to_geo(points_of_mpa_ships, box=True)
    geopoints_['pre'] = geopoints_['date'] < date
    # fastest way to do this is just this whole join:
    print('running sjoin on geopoints of mpa ships')
    geopoints_['in_mpa'] = ~geopandas.sjoin(geopoints_, mpa[['geometry']], how='left', op='within')['index_right'].isnull()
    # TODO - what about on the border/indeterminate?
    # geopoints_['intersects_mpa'] = ~geopandas.sjoin(geopoints_, mpa[['geometry']], how='left', op='intersects')['index_right'].isnull()
    aggregated_ = geopoints_.groupby(['mmsi', 'in_mpa', 'pre'], as_index=False).aggregate({'fishing_hours': 'sum', 'mmsi': 'first'})
    aggregated_['in_pre'] = aggregated_.apply(lambda row: row['fishing_hours'] if (row['in_mpa'] and row['pre']) else 0.0, axis=1)
    aggregated_['out_pre'] = aggregated_.apply(lambda row: row['fishing_hours'] if (not row['in_mpa'] and row['pre']) else 0.0, axis=1)
    aggregated_['in_post'] = aggregated_.apply(lambda row: row['fishing_hours'] if (row['in_mpa'] and not row['pre']) else 0.0, axis=1)
    aggregated_['out_post'] = aggregated_.apply(lambda row: row['fishing_hours'] if (not row['in_mpa'] and not row['pre']) else 0.0, axis=1)
    table = aggregated_.groupby(['mmsi'], as_index=False).aggregate({'in_pre': 'sum', 'out_pre': 'sum', 'in_post': 'sum', 'out_post': 'sum'})
    table['pre_percent_in'] = table['in_pre'] / (table['in_pre'] + table['out_pre'])
    table['post_percent_in'] = table['in_post'] / (table['in_post'] + table['out_post'])
    return table

   

def plot_effort_with_world(effort, mpa, linewidth=0.5, title=''):
    world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    ax = world.plot(color='white', edgecolor='black', figsize=(20, 30))
    effort.plot(column='fishing_hours', cmap='Blues', scheme='quantiles', ax=ax, legend=True)
    mpa.plot(ax=ax, color='None', edgecolor='red', linewidth=linewidth, alpha=0.5)
    plt.title(title)


def analyze_mpa(geopoints_sampled, full_points, mpa, date, plot_pre_post=True):
    #mpa = mpas[mpas['WDPAID'] == 555577562.0]

    print('running sjoin... ', end='')
    mpa_points = geopandas.sjoin(geopoints_sampled, mpa, op='within')
    print('\nfound {} sampled points in the mpa from {} ships'.format(
        mpa_points.shape[0], mpa_points['mmsi'].nunique()))

    points_of_mpa_ships = geopandas.GeoDataFrame(full_points[full_points['mmsi'].isin(mpa_points['mmsi'])])

    print('found {} points of mpa ships'.format(points_of_mpa_ships.shape[0]))
    
    table = table_of_in_out_pre_post(points_of_mpa_ships, mpa, date,).sort_values('in_pre', ascending=False)
    
    pre = convert_to_geo(
        join_on_lat_lon(points_of_mpa_ships[points_of_mpa_ships['date'] < date]), box=True)
    post = convert_to_geo(
        join_on_lat_lon(points_of_mpa_ships[points_of_mpa_ships['date'] >= date]), box=True)
    
    if plot_pre_post:
        plot_effort_with_world(pre, mpa, title='effort pre-closure')
        plot_effort_with_world(post, mpa, title='effort post-closure')
 
    return table, pre, post

