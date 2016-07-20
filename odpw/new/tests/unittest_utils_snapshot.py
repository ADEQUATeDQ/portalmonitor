import datetime
import unittest

from new.utils.utils_snapshot import tofirstdayinisoweek, getSnapshotfromTime


class SnapshotMethods(unittest.TestCase):

    def test_tofirstdayinisoweek(self):
        self.assertEqual(tofirstdayinisoweek(1620),datetime.datetime(2016, 5, 16, 0, 0))
        self.assertEqual(tofirstdayinisoweek(1627),datetime.datetime(2016, 7, 4, 0, 0))

    def test_getSnapshotfromTime(self):
        self.assertEqual(getSnapshotfromTime(datetime.datetime(2016, 5, 16, 0, 0)),1620)
        self.assertEqual(getSnapshotfromTime(datetime.datetime(2016, 7, 8, 0, 0)),1627)

    def test_split(self):
        s = 'hello world'
        self.assertEqual(s.split(), ['hello', 'world'])
        # check that s.split fails when the separator is not a string
        with self.assertRaises(TypeError):
            s.split(2)

if __name__ == '__main__':
    unittest.main()