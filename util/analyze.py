import numpy as np
import pandas
from datetime import datetime
import matplotlib.pyplot as plt
import os
import pywdpa
import geopandas
import contextily as ctx
from shapely import geometry
from shapely import ops

"""
NOTE: just changed 'op=...' to 'predicate=...' because of this:
FutureWarning: The `op` parameter is deprecated and will be removed in a future release.
Please use the `predicate` parameter instead.
  pipa_res = util.analyze_mpa(geopoints_sampled, points_by_year, pipa, '2013-01-01')
"""

class MpaResults():
    """
    Container class for all the intermediate results of different analysis
    """
    def __init__(self, mpa_frame, date, name, ):
        pass

def join_on_lat_lon(points):
    return points.groupby(['cell_ll_lat', 'cell_ll_lon'], as_index=False).aggregate(
        {'cell_ll_lat': 'first', 'cell_ll_lon': 'first', 'fishing_hours': 'sum'})


## Compute geometries for the points
def compute_box(row, width=0.1):
    x = float(row['cell_ll_lon'])
    y = float(row['cell_ll_lat'])
    return geometry.box(x, y, x+width, y+width)


def convert_to_geo(points, box=True):
    if box:
        points['geometry'] = points.apply(compute_box, axis=1)
        return geopandas.GeoDataFrame(points, geometry=points['geometry']).set_crs(epsg=4326)
    # use points (not as precise, but cheaper)
    else:
        return geopandas.GeoDataFrame(
            points,
            geometry=geopandas.points_from_xy(
                points['cell_ll_lon']*0.1 + 0.05, points['cell_ll_lat']*0.1 + 0.05)
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
    geopoints_['in_mpa'] = ~geopandas.sjoin(
        geopoints_, mpa[['geometry']], how='left', predicate='within')['index_right'].isnull()
    

    # TODO - what about on the border/indeterminate?
    # geopoints_['intersects_mpa'] = ~geopandas.sjoin(
    #   geopoints_, mpa[['geometry']], how='left', predicate='intersects')['index_right'].isnull()
    aggregated_ = geopoints_.groupby(['mmsi', 'in_mpa', 'pre'], as_index=False).aggregate(
        {'fishing_hours': 'sum', 'mmsi': 'first'})
    aggregated_['in_pre'] = aggregated_.apply(
        lambda row: row['fishing_hours'] if (row['in_mpa'] and row['pre']) else 0.0, axis=1)
    aggregated_['out_pre'] = aggregated_.apply(
        lambda row: row['fishing_hours'] if (not row['in_mpa'] and row['pre']) else 0.0, axis=1)
    aggregated_['in_post'] = aggregated_.apply(
        lambda row: row['fishing_hours'] if (row['in_mpa'] and not row['pre']) else 0.0, axis=1)
    aggregated_['out_post'] = aggregated_.apply(
        lambda row: row['fishing_hours'] if (not row['in_mpa'] and not row['pre']) else 0.0, axis=1)
    table = aggregated_.groupby(['mmsi'], as_index=False).aggregate(
        {'in_pre': 'sum', 'out_pre': 'sum', 'in_post': 'sum', 'out_post': 'sum'})
    table['pre_percent_in'] = table['in_pre'] / (table['in_pre'] + table['out_pre'])
    table['post_percent_in'] = table['in_post'] / (table['in_post'] + table['out_post'])
    return table, geopoints_

   

def plot_effort_with_world(effort, mpa, linewidth=0.5, title='', filename=None):
    fig = plt.figure()
    world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    ax = world.plot(color='white', edgecolor='black', figsize=(20, 30))
    effort.plot(column='fishing_hours', cmap='Blues', scheme='quantiles', ax=ax, legend=True)
    mpa.plot(ax=ax, color='None', edgecolor='red', linewidth=linewidth, alpha=0.5)
    plt.title(title)
    if filename:
        plt.savefig(filename)
        #pl.clf()
    plt.close(fig)

def plot_effort_without_world(effort, mpa, linewidth=0.5, title='', filename=None):
    fig = plt.figure()
    world = geopandas.read_file(geopandas.datasets.get_path('naturalearth_lowres'))
    ax = effort.plot(column='fishing_hours', cmap='Blues', scheme='quantiles', legend=True)
    mpa.plot(ax=ax, color='None', edgecolor='red', linewidth=linewidth, alpha=0.5)
    plt.title(title)
    if filename:
        plt.savefig(filename)
        #pl.clf()
    plt.close(fig)

def points_of_interest(geopoints_sampled, points_by_year, mpa, date, verbose=False):
    if verbose:
        print('running sjoin... ', end='')
    mpa_points = geopandas.sjoin(geopoints_sampled, mpa, predicate='within')

    # mpa_points = mpa_points[mpa_points['date'] < date] # probably shouldn't leave this in

    if verbose:
        print('\nfound {} sampled points in the mpa from {} ships'.format(
            mpa_points.shape[0], mpa_points['mmsi'].nunique()))

    if mpa_points.shape[0] == 0:
        if verbose:
            print('exiting: found no points')
        return

    points_of_mpa_ships = []
    for _, points in points_by_year.items():
        points_of_mpa_ships.append(points[points['mmsi'].isin(mpa_points['mmsi'])])
    points_of_mpa_ships = geopandas.GeoDataFrame(pandas.concat(points_of_mpa_ships))

    if verbose:
        print('found {} points of mpa ships'.format(points_of_mpa_ships.shape[0]))
    return points_of_mpa_ships


def plot_in_out_lines(points_of_mpa_ships, mpa_name, date, window=30, folder=None, individuals=False):
    start = np.datetime64('2012-01-01')
    end = np.datetime64('2021-01-01')
    days_in_interval = (end - start).item().days
    X = np.arange(start + np.timedelta64(window-1, 'D'), end, np.timedelta64(1, 'D')).astype(datetime)


    def _avg(x, w):
        return np.convolve(x, np.ones(w), 'valid') / w

    def _plot(points, filename, title):
        effort_in = np.zeros(days_in_interval)
        effort_out = np.zeros(days_in_interval)
        for i, row in points.iterrows():
            d = (row['date'] - start).days
            if row['in_mpa']:
                effort_in[d] += row['fishing_hours']
            else:
                effort_out[d] += row['fishing_hours']
        fig = plt.figure()
        plt.plot(X, _avg(effort_in, window), label='in')
        plt.plot(X, _avg(effort_out, window), label='out')
        plt.axvline(np.datetime64(date), c='black', ls=':')
        plt.ylabel(f'fishing hours – {window}-day average')
        plt.legend(loc='upper right')
        plt.title(title)
        plt.savefig(filename)
        plt.close(fig)

    if individuals:
        mmsis = points_of_mpa_ships['mmsi'].unique()
        for mmsi in mmsis:
            points = points_of_mpa_ships[points_of_mpa_ships['mmsi'] == mmsi]
            filename = folder+'/by_mmsi/'+str(mmsi)
            _plot(points, filename, f'{mmsi} – {mpa_name}')

    _plot(points_of_mpa_ships, folder+f'/{mpa_name}', f'{mpa_name}')
    

def draw_maps(points_of_mpa_ships, mpa_name, date, mpa, folder=None, individuals=False):

    def _draw(points, filename, title):
        pre, post = points[points['date'] < date], points[points['date'] >= date]
        if pre.shape[0] > 0:
            pre = convert_to_geo(join_on_lat_lon(pre), box=True)
            plot_effort_with_world(pre, mpa, title=title+' pre',
                    filename=filename+'_pre')
        if post.shape[0] > 0:
            pre = convert_to_geo(join_on_lat_lon(post), box=True)
            plot_effort_with_world(post, mpa, title=title+' post',
                    filename=filename+'_post')


    if individuals:
        mmsis = points_of_mpa_ships['mmsi'].unique()
        for mmsi in mmsis:
            points = points_of_mpa_ships[points_of_mpa_ships['mmsi'] == mmsi]
            filename = folder+'/by_mmsi/'+str(mmsi)+'_map'
            _draw(points, filename, f'{mmsi} – {mpa_name}')



def analyze_mpa(geopoints_sampled, points_by_year, mpa, date, name,
    plot=False, verbose=False, folder=None, total_effort_plots=False):
    #mpa = mpa.dissolve(by='WDPAID')
    #mpa = mpa.dissolve(by='wdpa_id')

    points_of_mpa_ships = points_of_interest(
        geopoints_sampled, points_by_year, mpa, date, verbose=verbose)
    if points_of_mpa_ships is None:
        return

    table, points_of_mpa_ships = table_of_in_out_pre_post(
        points_of_mpa_ships, mpa, date,)
    table = table.sort_values('in_pre', ascending=False)


    
    if plot:
        try:
            os.makedirs(folder)
            #os.makedirs(folder+'/by_mmsi')
        except:
            pass

        plot_in_out_lines(points_of_mpa_ships, name, date, folder=folder)
        draw_maps(points_of_mpa_ships, name, date, mpa, folder=folder)


        pre = points_of_mpa_ships[points_of_mpa_ships['date'] < date]
        post = points_of_mpa_ships[points_of_mpa_ships['date'] >= date]
        if total_effort_plots:
            if (pre.shape[0] > 0):
                pre = convert_to_geo(
                    join_on_lat_lon(points_of_mpa_ships[points_of_mpa_ships['date'] < date]), box=True)
                filename=None
                if folder:
                    plot_effort_with_world(pre, mpa, title=f'{name} effort pre-closure ({date})',
                        filename=folder+'pre_closure_effort_with_world')
                    plot_effort_without_world(pre, mpa, title=f'{name} effort pre-closure ({date})',
                        filename=folder+'pre_closure_effort')

            if (post.shape[0] > 0):
                post = convert_to_geo(
                    join_on_lat_lon(points_of_mpa_ships[points_of_mpa_ships['date'] >= date]), box=True)
                filename=None
                if folder:
                    plot_effort_with_world(post, mpa, title=f'{name} effort post-closure ({date})',
                        filename=folder+'post_closure_effort_with_world')
                    plot_effort_without_world(post, mpa, title=f'{name} effort post-closure ({date})',
                        filename=folder+'post_closure_effort')
            



    return table, points_of_mpa_ships











