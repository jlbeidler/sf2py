from datetime import date
import unittest
from unittest.mock import patch

import geopandas as gpd
from shapely import wkb
from exports import Export
from shapely.geometry import Point, Polygon, box, shape


class DailyFireExtentTests(unittest.TestCase):
    def test_normalizes_overallocated_daily_areas_proportionally(self):
        first = Export._daily_target_area(100.0, 60.0, 100.0, 160.0)
        second = Export._daily_target_area(100.0, 100.0, 100.0, 160.0)

        self.assertAlmostEqual(first, 37.5)
        self.assertAlmostEqual(second, 62.5)
        self.assertAlmostEqual(first + second, 100.0)

    def test_daily_export_allocates_disjoint_proportional_polygons(self):
        class Connection:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        class Engine:
            def connect(self):
                return Connection()

        class Database:
            engine = Engine()

        exporter = Export.__new__(Export)
        outline = box(0, 0, 100, 100)
        previous_area_by_event = {}
        features = []

        for day, event_day_id, clump in (
            (date(2026, 1, 1), 1, box(20, 45, 30, 55)),
            (date(2026, 1, 2), 2, box(70, 45, 80, 55)),
        ):
            daily_rows = gpd.GeoDataFrame([{
                'event_id': 7,
                'event_name': 'Test',
                'event_start_date': date(2026, 1, 1),
                'event_end_date': date(2026, 1, 2),
                'fire_type': 'test',
                'event_area': 10000.0,
                'event_day_id': event_day_id,
                'daily_area': 2500.0,
                'total_daily_area': 5000.0,
                'clump_wkb': wkb.dumps(clump),
                'clump_daily_area': 2500.0,
                'longitude': 0.0,
                'latitude': 0.0,
                'outline_shape': outline,
            }], geometry='outline_shape', crs='EPSG:3857')
            with patch('exports.gpd.read_postgis', return_value=daily_rows):
                result = exporter._get_daily_fire_extent_shape(
                    [7], day, Database(), previous_area_by_event
                )
            features.extend(result['features'])

        self.assertEqual(len(features), 2)
        for feature in features:
            self.assertAlmostEqual(
                feature['properties']['daily_area_sqm'], 2500.0, delta=10.0
            )
        self.assertAlmostEqual(
            previous_area_by_event[7][1].area, 5000.0, delta=10.0
        )

    def test_allocates_requested_area_near_clump_center(self):
        outline = box(0, 0, 100, 100)

        polygon = Export._allocate_daily_shape([Point(40, 40)], outline, 25.0)

        self.assertIsNotNone(polygon)
        self.assertAlmostEqual(polygon.area, 25.0, delta=10.0)
        self.assertTrue(outline.covers(polygon))

    def test_weights_allocation_by_clump_area(self):
        outline = box(0, 0, 100, 100)
        centers = [Point(25, 50), Point(75, 50)]

        polygon = Export._allocate_daily_shape(
            centers, outline, 400.0, [100.0, 400.0]
        )

        self.assertIsNotNone(polygon)
        left_area = polygon.intersection(box(0, 0, 50, 100)).area
        right_area = polygon.intersection(box(50, 0, 100, 100)).area
        self.assertAlmostEqual(right_area / left_area, 4.0, delta=0.1)

    def test_export_weights_clump_locations_by_clump_area(self):
        class Connection:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        class Engine:
            def connect(self):
                return Connection()

        class Database:
            engine = Engine()

        exporter = Export.__new__(Export)
        outline = box(0, 0, 100, 100)
        daily_rows = gpd.GeoDataFrame([
            {
                'event_id': 7,
                'event_name': 'Test',
                'event_start_date': date(2026, 1, 1),
                'event_end_date': date(2026, 1, 1),
                'fire_type': 'test',
                'event_area': 10000.0,
                'event_day_id': event_day_id,
                'daily_area': 500.0,
                'total_daily_area': 1000.0,
                'clump_wkb': wkb.dumps(clump),
                'clump_daily_area': clump_daily_area,
                'longitude': 0.0,
                'latitude': 0.0,
                'outline_shape': outline,
            }
            for event_day_id, clump, clump_daily_area in (
                (1, box(20, 45, 30, 55), 100.0),
                (2, box(70, 45, 80, 55), 400.0),
            )
        ], geometry='outline_shape', crs='EPSG:3857')

        with patch('exports.gpd.read_postgis', return_value=daily_rows):
            result = exporter._get_daily_fire_extent_shape(
                [7], date(2026, 1, 1), Database()
            )

        geometry = shape(result['features'][0]['geometry'])
        polygon = gpd.GeoSeries(
            [geometry], crs='EPSG:4326'
        ).to_crs('EPSG:3857').iloc[0]
        left_area = polygon.intersection(box(0, 0, 50, 100)).area
        right_area = polygon.intersection(box(50, 0, 100, 100)).area
        self.assertAlmostEqual(right_area / left_area, 4.0, delta=0.1)
        self.assertTrue(polygon.covers(Point(25, 50)))
        self.assertTrue(polygon.covers(Point(75, 50)))

    def test_output_contains_associated_clump_centroid(self):
        class Connection:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

        class Engine:
            def connect(self):
                return Connection()

        class Database:
            engine = Engine()

        exporter = Export.__new__(Export)
        outline = box(0, 0, 100, 100)
        clump = Polygon([(10, 10), (90, 10), (10, 11)])
        centroid = clump.centroid
        daily_rows = gpd.GeoDataFrame([{
            'event_id': 7,
            'event_name': 'Test',
            'event_start_date': date(2026, 1, 1),
            'event_end_date': date(2026, 1, 1),
            'fire_type': 'test',
            'event_area': 10000.0,
            'event_day_id': 1,
            'daily_area': 25.0,
            'total_daily_area': 10000.0,
            'clump_wkb': wkb.dumps(clump),
            'clump_daily_area': 25.0,
            'longitude': 0.0,
            'latitude': 0.0,
            'outline_shape': outline,
        }], geometry='outline_shape', crs='EPSG:3857')

        with patch('exports.gpd.read_postgis', return_value=daily_rows):
            result = exporter._get_daily_fire_extent_shape(
                [7], date(2026, 1, 1), Database()
            )

        geometry = shape(result['features'][0]['geometry'])
        polygon = gpd.GeoSeries(
            [geometry], crs='EPSG:4326'
        ).to_crs('EPSG:3857').iloc[0]
        self.assertTrue(polygon.covers(centroid))

    def test_allocates_compact_area_around_clump_location(self):
        outline = box(0, 0, 100, 100)

        polygon = Export._allocate_daily_shape(
            [Point(50, 50)], outline, 1000.0
        )

        self.assertIsNotNone(polygon)
        min_x, min_y, max_x, max_y = polygon.bounds
        self.assertGreater(
            min(max_x - min_x, max_y - min_y)
            / max(max_x - min_x, max_y - min_y),
            0.95,
        )

    def test_allocations_on_successive_days_do_not_overlap(self):
        outline = box(0, 0, 100, 100)
        center = Point(50, 50)

        first = Export._allocate_daily_shape([center], outline, 100.0)
        remaining = outline.difference(first)
        second = Export._allocate_daily_shape([center], remaining, 100.0)

        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        self.assertAlmostEqual(first.area, 100.0, delta=0.01)
        self.assertAlmostEqual(second.area, 100.0, delta=0.01)
        self.assertAlmostEqual(first.intersection(second).area, 0.0, delta=0.01)

    def test_prefers_one_contiguous_component_when_it_can_fit_target(self):
        available = box(0, 0, 10, 10).union(box(20, 0, 30, 10))

        polygon = Export._allocate_daily_shape([Point(5, 5)], available, 25.0)

        self.assertIsNotNone(polygon)
        self.assertEqual(polygon.geom_type, 'Polygon')
        self.assertAlmostEqual(polygon.area, 25.0, delta=10.0)

    def test_uses_multiple_components_only_when_one_cannot_fit_target(self):
        available = box(0, 0, 10, 10).union(box(20, 0, 30, 10))
        centers = [Point(5, 5), Point(25, 5)]

        polygon = Export._allocate_daily_shape(centers, available, 150.0)

        self.assertIsNotNone(polygon)
        self.assertEqual(polygon.geom_type, 'MultiPolygon')
        self.assertAlmostEqual(polygon.area, 150.0, delta=0.01)

    def test_includes_every_clump_center_across_disconnected_regions(self):
        available = box(0, 0, 10, 10).union(box(20, 0, 30, 10))
        centers = [Point(5, 5), Point(25, 5)]

        polygon = Export._allocate_daily_shape(centers, available, 25.0)

        self.assertIsNotNone(polygon)
        self.assertTrue(all(polygon.covers(center) for center in centers))
        self.assertEqual(polygon.geom_type, 'MultiPolygon')
        self.assertLess(polygon.area, 50.0)

    def test_simplifies_daily_shape_with_one_meter_threshold(self):
        outline = Polygon([
            (0, 0), (10, 0), (10, 10), (8, 10), (7, 10.4), (6, 10),
            (5, 10.4), (4, 10), (3, 10.4), (2, 10), (0, 10), (0, 0),
        ])

        polygon = Export._simplify_daily_shape(outline)

        self.assertIsNotNone(polygon)
        self.assertLess(len(polygon.exterior.coords), len(outline.exterior.coords))
        self.assertLessEqual(polygon.hausdorff_distance(outline), 1.0)

    def test_caps_target_to_remaining_area(self):
        available = box(0, 0, 10, 10)

        polygon = Export._allocate_daily_shape([Point(5, 5)], available, 200.0)

        self.assertEqual(polygon, available)


if __name__ == '__main__':
    unittest.main()
