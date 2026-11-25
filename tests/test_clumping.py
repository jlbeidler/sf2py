from datetime import date
import unittest
from unittest.mock import patch

import geopandas as gpd
from shapely.geometry import Point

from sf2py.clump import Clump


class FakeDatabase:
    def __init__(self, detections):
        self.detections = detections

    def read_geodataframe(self, query, geom_col):
        return self.detections.copy()


class SynchronousPool:
    def __init__(self, processes):
        self.processes = processes

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    def map(self, function, chunks):
        return [function(chunk) for chunk in chunks]

    def close(self):
        pass


class ClumpingTests(unittest.TestCase):
    def test_daily_clumps_use_radius_neighbors_and_buffer_their_envelope(self):
        detections = gpd.GeoDataFrame(
            {
                'id': [1, 2, 3],
                'area': [1.0, 1.0, 1.0],
                'start_date': [date(2026, 1, 1)] * 3,
                'end_date': [date(2026, 1, 1)] * 3,
                'shape': [Point(0, 0), Point(10, 0), Point(30, 0)],
            },
            geometry='shape',
            crs='EPSG:3857',
        )
        clumper = Clump({
            'clump_method': 'default',
            'clumping': {'radius': 10},
        })

        def assign_ids(db):
            clumper.clumps['id'] = range(1, len(clumper.clumps) + 1)

        clumper._set_area = lambda: None
        clumper._write_clump_data = assign_ids
        clumper._update_raw_id = lambda db, source_id: None

        with patch('sf2py.clump.Pool', SynchronousPool):
            clumper.clump(FakeDatabase(detections), 'source')

        self.assertEqual(len(clumper.clumps), 2)
        cluster_id = clumper.srcmap.loc[
            clumper.srcmap['rawdata_id'] == 1, 'tmp_clump'
        ].iloc[0]
        isolated_id = clumper.srcmap.loc[
            clumper.srcmap['rawdata_id'] == 3, 'tmp_clump'
        ].iloc[0]
        self.assertEqual(
            clumper.srcmap.loc[
                clumper.srcmap['rawdata_id'] == 2, 'tmp_clump'
            ].iloc[0],
            cluster_id,
        )
        self.assertNotEqual(cluster_id, isolated_id)
        first_cluster = clumper.clumps.loc[
            clumper.clumps['tmp_clump'] == cluster_id, 'shape'
        ].iloc[0]
        isolated_detection = clumper.clumps.loc[
            clumper.clumps['tmp_clump'] == isolated_id, 'shape'
        ].iloc[0]
        expected_cluster = (
            Point(0, 0)
            .union(Point(10, 0))
            .envelope.buffer(10, resolution=24)
        )
        expected_isolated = Point(30, 0).buffer(10, resolution=24)

        self.assertTrue(first_cluster.equals(expected_cluster))
        self.assertTrue(isolated_detection.equals(expected_isolated))


if __name__ == '__main__':
    unittest.main()
