import unittest
import tempfile
import os
import time

import pandas as pd

import sys

sys.path.append(".")

from track_results.track_results_json import TrackResultsJSON


class TestTrackResults(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp_dir = tempfile.TemporaryDirectory()
        cls.filename = os.path.join(cls.temp_dir.name, "test_results.json")
        print(
            f"TestTrackResults.setUp: dir={cls.temp_dir.name}, filename={cls.filename}"
        )
        cls.tracker = TrackResultsJSON(cls.filename)

    @classmethod
    def tearDownClass(cls):
        print(f"TestTrackResults.tearDown: dir={cls.temp_dir.name}")
        cls.temp_dir.cleanup()

    def test_add_and_query(self):
        t0 = time.perf_counter()

        print("track @ 0\n", self.tracker)

        t0 = time.perf_counter()
        time.sleep(1)
        dt = time.perf_counter()

        t1 = pd.Timestamp.now()

        # Add first record
        params1 = {"lr": 0.01, "opt": "adam", "beta": [0.99, 0.99]}
        results1 = {"acc": 0.9, "time": t1, "dt": dt}
        self.tracker.add(params1, results1)
        df = self.tracker.get()
        print(df.keys())
        print(df)
        self.assertEqual(
            df.shape, (1, 3 + 3 + 1 + 9)
        )  # parameters+results+date+platform

        print("track @ 1\n", self.tracker)

        # Add second record
        params2 = {"lr": 0.02, "opt": "sgd"}
        results2 = {"acc": 0.8}
        self.tracker.add(params2, results2)
        df = self.tracker.get()
        print(df.keys())
        print(df)
        self.assertEqual(
            df.shape, (2, 3 + 3 + 1 + 9)
        )  # parameters+results+date+platform

        print("track @ 2\n", self.tracker)

        # Add third record (repeated so replace)
        params2 = {"lr": 0.02, "opt": "sgd"}
        results2 = {"acc": 0.7}
        self.tracker.add(params2, results2, replace=True)
        df = self.tracker.get()
        print(df.keys())
        print(df)
        self.assertEqual(
            df.shape, (2, 3 + 3 + 1 + 9)
        )  # parameters+results+date+platform

        # Add forth record (repeated but does not replace)
        params2 = {"lr": 0.02, "opt": "sgd"}
        results2 = {"acc": 0.6}
        self.tracker.add(params2, results2, replace=False)

        print(f"test_add() {time.perf_counter()-t0} sec")

    def test_query(self):

        t0 = time.perf_counter()

        print("track @ 3\n", self.tracker)

        # Test querying all data
        df = self.tracker.get()
        print(df.keys())
        print(df)
        self.assertEqual(
            df.shape, (3, 3 + 3 + 1 + 9)
        )  # parameters+results+date+platform
        # Verify flattening works
        self.assertIn("parameters.lr", df.columns)
        self.assertIn("results.acc", df.columns)
        self.assertIn("date", df.columns)
        # verify replace works
        self.assertEqual(df.iloc[1]["results.acc"], 0.7)
        self.assertEqual(df.iloc[2]["results.acc"], 0.6)

        # Test querying with a filter string
        # Note: using backticks for columns with dots
        df_filtered = self.tracker.get("`parameters.lr` == 0.01")
        print(df_filtered)
        self.assertEqual(len(df_filtered), 1)
        self.assertEqual(df_filtered.iloc[0]["parameters.opt"], "adam")

        # Test querying specific columns
        df_cols = self.tracker.get(columns={"results.acc": "res.acc"})
        print(df_cols)
        self.assertListEqual(list(df_cols.columns), ["res.acc"])
        self.assertEqual(len(df_cols), 3)

        # Test time filtering
        # Select results older than 1 hour ago (so all kept)
        past_time = pd.Timestamp.now() - pd.Timedelta(hours=1)
        df_past1 = self.tracker.get(last_time=past_time)
        print(df_past1)
        self.assertEqual(len(df_past1), 3)

        # Select results older than 0 hour ago (so none kept)
        past_time = pd.Timestamp.now() - pd.Timedelta(hours=0)
        df_past0 = self.tracker.get(last_time=past_time)
        print(df_past0)
        self.assertEqual(len(df_past0), 0)

        # Select results older than 1 hour ago (so all kept)
        past_interval = pd.Timedelta(hours=1)
        df_future = self.tracker.get(time_interval=past_interval)
        self.assertEqual(len(df_future), 3)

        # Select results older than 0 hour ago (so none kept)
        past_interval = -pd.Timedelta(hours=1)
        df_future = self.tracker.get(time_interval=past_interval)
        self.assertEqual(len(df_future), 0)

        print(f"test_query() {time.perf_counter()-t0} sec")

    def test_remove(self):

        t0 = time.perf_counter()

        print("track @ 4\n")  # , self.tracker)

        # Test querying all data
        df = self.tracker.get()
        # print(df.keys())
        print(df)
        self.assertEqual(len(df), 3)

        # nothing matches query
        self.tracker.remove(query="`results.acc` == 1.0", simulate=True)
        df = self.tracker.get()
        self.assertEqual(len(df), 3)
        self.tracker.remove(query="`results.acc` == 1.0", simulate=False)
        df = self.tracker.get()
        self.assertEqual(len(df), 3)

        # 1 record matches query
        self.tracker.remove(query="`results.acc` == 0.9", simulate=True)
        df = self.tracker.get()
        self.assertEqual(len(df), 3)
        self.tracker.remove(query="`results.acc` == 0.9", simulate=False)
        df = self.tracker.get()
        self.assertEqual(len(df), 2)

        # 2 records match query
        self.tracker.remove(
            query="`platform.node`=='dorabella.local' and `platform.architecture`==\"('64bit', '')\" and `platform.machine`==\"x86_64\"",
            simulate=True,
        )
        df = self.tracker.get()
        self.assertEqual(len(df), 2)
        self.tracker.remove(
            query="`platform.node`=='dorabella.local' and `platform.architecture`==\"('64bit', '')\" and `platform.machine`==\"x86_64\"",
            simulate=False,
        )
        df = self.tracker.get()
        self.assertEqual(len(df), 0)

        print(f"test_remove() {time.perf_counter()-t0} sec")


if __name__ == "__main__":
    unittest.main()
