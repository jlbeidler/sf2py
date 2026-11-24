import unittest

from exports import Export
from shapely.geometry import box


class DailyFireExtentTests(unittest.TestCase):
    def test_keeps_activity_when_area_cannot_be_allocated_near_boundary(self):
        outline = box(0, 0, 100, 100)
        activity = box(98, 49, 99, 50)

        polygon = Export._allocate_daily_shape(activity, outline, 25.0)
        expected = Export._clean_daily_shape(activity.intersection(outline), outline)

        self.assertIsNotNone(polygon)
        self.assertEqual(polygon, expected)
        self.assertTrue(outline.covers(polygon))

    def test_scales_activity_when_area_can_be_allocated(self):
        outline = box(0, 0, 100, 100)
        activity = box(40, 40, 41, 41)

        polygon = Export._allocate_daily_shape(activity, outline, 25.0)

        self.assertIsNotNone(polygon)
        self.assertAlmostEqual(polygon.area, 25.0, delta=0.01)


if __name__ == '__main__':
    unittest.main()
