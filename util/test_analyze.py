import unittest
import pandas
import geopandas

import analyze

## These are meant to be inside the PIPA mpa:
points_2012 = pandas.DataFrame({
    'date': ['2012-01-01', '2012-01-02'],
    'cell_ll_lat': [-2, -2],
    'cell_ll_lon': [-171, -171],
    'mmsi': [1, 2],
    'hours': [5, 5],
    'fishing_hours': [10, 20],
})

points_2013 = pandas.DataFrame({
    'date': ['2013-01-01', '2013-01-02'],
    'cell_ll_lat': [-2, -2],
    'cell_ll_lon': [-171, -171],
    'mmsi': [1, 2],
    'hours': [5, 5],
    'fishing_hours': [10, 20],
})

## TODO - use a smaller set of mpas probably
mpas = geopandas.read_parquet("/Users/brendan/Masters/gfw_research/data/mpas.parquet")
pipa = mpas[mpas['WDPAID'] == 555512002.0]
coral_park = mpas[mpas['WDPAID'] == 555577562.0]
marae_moana = mpas[mpas['WDPAID'] == 555624907.0]

class AnalyzeConvertToGeo(unittest.TestCase):

    def test_convert_to_geo(self):
        geo_2012 = analyze.convert_to_geo(points_2012)
        self.assertTrue(geo_2012.is_valid.all())


class AnalyzeMpaTest(unittest.TestCase):

    def setUp(self):
        self.geopoints_sampled = analyze.convert_to_geo(pandas.concat([points_2012, points_2013]))
        self.points_by_year = {2012: points_2012, 2013: points_2013}

    # Finds four points because they're all set to be inside PIPA.
    def test_analyze_pipa_runs(self):
        analyze.analyze_mpa(self.geopoints_sampled, self.points_by_year, pipa, '2012-06-01', plot_pre_post=False)

    
    #Should find no points and exit safely.
    def test_analyze_coral_park_runs(self):
        analyze.analyze_mpa(self.geopoints_sampled, self.points_by_year, coral_park, '2012-06-01', plot_pre_post=False)


    # def test_multiple_geometries(self):
    #     analyze.analyze_mpa(self.geopoints_sampled, self.points_by_year, marae_moana, '2012-06-01', plot_pre_post=False)
