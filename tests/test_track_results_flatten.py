import unittest
import time

import pandas as pd
import numpy as np

import sys

sys.path.append(".")

from track_results import TrackResults

try:
    # the file my_secrets.py should create a variable `MONGODB_URI` with the URI of the MongoDB
    import my_secrets

    MONGODB_URI = my_secrets.MONGODB_URI
except:
    # if the file does not exist, will default to a mongita database
    MONGODB_URI = None


class TestTrackResults(unittest.TestCase):

    @classmethod
    def setUpClass(cls, uri=None):
        cls.collection = "test_results"
        cls.uri = MONGODB_URI
        cls.uri = None
        print(
            f"TestTrackResults.setUpClass: uri={cls.uri}, collection={cls.collection}"
        )
        cls.tracker = TrackResults(
            uri=cls.uri,
            collection=cls.collection,
            verbose=True,
        )
        cls.tracker.drop(simulate=False)  # Ensure a clean slate for each test

    @classmethod
    def tearDownClass(cls):
        print(f"TestTrackResults.tearDownClass: collection={cls.tracker.collection}")
        cls.tracker.drop(simulate=False)

    def test_add(self):

        self.tracker.remove(filter={}, simulate=False)

        t0 = time.perf_counter()

        print("track @ 0\n", self.tracker)

        t0 = time.perf_counter()
        time.sleep(1)
        dt = time.perf_counter()

        t1 = pd.Timestamp.now()

        # Add first record
        params1 = {"lr": 0.01, "opt": "adam", "beta": [0.99, 0.99]}
        results1 = {"acc": 0.9, "time": t1, "dt": dt}
        self.tracker.add(params1, results1, replace=True, flatten=True)
        df = self.tracker.get()
        # print(df.keys())
        print(df)
        self.assertEqual(
            df.shape, (1, 3 + 3 + 1 + 9)
        )  # parameters+results+date+platform
        self.assertEqual(len(self.tracker), 1)

        print("track @ 1\n")  # , self.tracker)

        # Add second record
        params2 = {"lr": 0.02, "opt": "sgd"}
        results2 = {"acc": 0.8}
        self.tracker.add(params2, results2, flatten=True)
        df = self.tracker.get()
        # print(df.keys())
        print(df)
        self.assertEqual(
            df.shape, (2, 3 + 3 + 1 + 9)
        )  # parameters+results+date+platform
        self.assertEqual(len(self.tracker), 2)

        print("track @ 2\n")  # , self.tracker)

        # Add third record (repeated so replace)
        params2 = {"lr": 0.02, "opt": "sgd"}
        results2 = {"acc": 0.7}
        self.tracker.add(params2, results2, replace=True, flatten=True)
        df = self.tracker.get()
        # print(df.keys())
        print(df)
        self.assertEqual(
            df.shape, (2, 3 + 3 + 1 + 9)
        )  # parameters+results+date+platform
        self.assertEqual(len(self.tracker), 2)

        # Add forth record (repeated but does not replace)
        params2 = {"lr": 0.02, "opt": "sgd"}
        results2 = {"acc": 0.6}
        self.tracker.add(params2, results2, replace=False, flatten=True)
        df = self.tracker.get()
        # print(df.keys())
        print(df)
        self.assertEqual(
            df.shape, (3, 3 + 3 + 1 + 9)
        )  # parameters+results+date+platform
        self.assertEqual(len(self.tracker), 3)

        print(f"test_add() {time.perf_counter()-t0} sec")

        ## QUERY

        t0 = time.perf_counter()

        print("track @ 3\n")  # , self.tracker)

        # Test querying all data
        df = self.tracker.get()
        # print(df.keys())
        print(df)
        self.assertEqual(
            df.shape, (3, 3 + 3 + 1 + 9)
        )  # parameters+results+date+platform
        # Verify flattening works
        self.assertIn("parameters_lr", df.columns)
        self.assertIn("results_acc", df.columns)
        self.assertIn("date", df.columns)
        # verify replace works
        self.assertEqual(df.iloc[1]["results_acc"], 0.7)
        self.assertEqual(df.iloc[2]["results_acc"], 0.6)

        # Test querying with a filter string
        # Note: using backticks for columns with dots
        df_filtered = self.tracker.get(filter={"parameters_lr": 0.01})
        print(df_filtered)
        self.assertEqual(len(df_filtered), 1)
        self.assertEqual(df_filtered.iloc[0]["parameters_opt"], "adam")

        # Test querying specific columns
        df_cols = self.tracker.get(columns={"results_acc": "res_acc"})
        print(df_cols)
        self.assertListEqual(list(df_cols.columns), ["res_acc"])
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

        ## REMOVE

        print(f"TestTrackResults.test_remove: collection={self.tracker.collection}")

        t0 = time.perf_counter()

        print("track @ 4\n")  # , self.tracker)

        # Test querying all data
        df = self.tracker.get()
        # print(df.keys())
        print(df)
        self.assertEqual(len(df), 3)

        # nothing matches query
        self.tracker.remove(filter={"results_acc": 1.0}, simulate=True)
        df = self.tracker.get()
        self.assertEqual(len(df), 3)
        self.tracker.remove(filter={"results_acc": 1.0}, simulate=False)
        df = self.tracker.get()
        self.assertEqual(len(df), 3)

        # 1 record matches query
        self.tracker.remove(filter={"results_acc": 0.9}, simulate=True)
        df = self.tracker.get()
        self.assertEqual(len(df), 3)
        self.tracker.remove(filter={"results_acc": 0.9}, simulate=False)
        df = self.tracker.get()
        self.assertEqual(len(df), 2)

        # multi-query
        df = self.tracker.get(
            filter={
                "platform_architecture": ["64bit", ""],
                "platform_machine": "x86_64",
                "results_acc": {"$gte": 0.5},
            },
            columns={
                "platform_architecture": "arch",
                "platform_machine": "mach",
                "results_acc": "results",
            },
        )
        print(df)
        self.assertEqual(df.shape, (2, 3))

        # added record that will not match query
        params1 = {"lr": 0.01, "opt": "adam", "beta": [0.99, 0.99]}
        results1 = {"acc": 0.1}
        self.tracker.add(params1, results1, replace=True, flatten=True)
        df = self.tracker.get()
        self.assertEqual(len(df), 3)

        # 2 records match query
        self.tracker.remove(
            filter={
                "platform_architecture": ["64bit", ""],
                "platform_machine": "x86_64",
                "results_acc": {"$gte": 0.5},
            },
            simulate=True,
        )
        df = self.tracker.get()
        self.assertEqual(len(df), 3)
        self.tracker.remove(
            filter={
                "platform_architecture": ["64bit", ""],
                "platform_machine": "x86_64",
                "results_acc": {"$gte": 0.5},
            },
            simulate=False,
        )
        df = self.tracker.get()
        print(df)
        self.assertEqual(len(df), 1)

        print(f"test_remove() {time.perf_counter()-t0} sec")

    def test_interesting_columns(self):
        print(
            f"TestTrackResults.test_interesting_columns: collection={self.tracker.collection}"
        )

        # mix float and lists
        self.tracker.remove(filter={}, simulate=False)
        params1 = {"lr": 0.01, "opt": "adam", "beta": [0.99, 0.99]}
        results1 = {"time": pd.Timestamp.now()}
        params2 = {"lr": 0.02, "opt": "adam", "beta": [0.99, 0.99]}
        results2 = {"time": pd.Timestamp.now()}
        params3 = {"lr": 0.03, "opt": "adam", "beta": [0.99, 0.99]}
        results3 = {"time": pd.Timestamp.now()}

        self.tracker.add(params1, results1, replace=False, flatten=True)
        self.tracker.add(params2, results2, replace=False, flatten=True)
        self.tracker.add(params3, results3, replace=False, flatten=True)

        df = self.tracker.get(drop_constant_columns=False)
        print(df)
        assert df.shape == (3, 14)

        df = self.tracker.get(drop_constant_columns=True)
        print(df)
        assert df.shape == (3, 3)  # date, lr, time

        # mix float and nan and lists
        self.tracker.remove(filter={}, simulate=False)
        params1 = {"lr": 0.01, "opt": "adam", "beta": [0.99, 0.99]}
        results1 = {"time": pd.Timestamp.now()}
        params2 = {"lr": np.nan, "opt": "adam", "beta": [0.99, 0.99]}
        results2 = {"time": pd.Timestamp.now()}
        params3 = {"lr": np.nan, "opt": "adam", "beta": [0.99, 0.99]}
        results3 = {"time": pd.Timestamp.now()}

        self.tracker.add(params1, results1, replace=False, flatten=True)
        self.tracker.add(params2, results2, replace=False, flatten=True)
        self.tracker.add(params3, results3, replace=False, flatten=True)

        df = self.tracker.get(drop_constant_columns=False)
        print(df)
        assert df.shape == (3, 14)

        df = self.tracker.get(drop_constant_columns=True)
        print(df)
        assert df.shape == (3, 3)  # date, lr, time

        # mix nan and lists
        self.tracker.remove(filter={}, simulate=False)
        params1 = {"lr": np.nan, "opt": "adam", "beta": [0.99, 0.99]}
        results1 = {"time": pd.Timestamp.now()}
        params2 = {"lr": np.nan, "opt": "adam", "beta": [0.99, 0.99]}
        results2 = {"time": pd.Timestamp.now()}
        params3 = {"lr": np.nan, "opt": "adam", "beta": [0.99, 0.99]}
        results3 = {"time": pd.Timestamp.now()}

        self.tracker.add(params1, results1, replace=False, flatten=True)
        self.tracker.add(params2, results2, replace=False, flatten=True)
        self.tracker.add(params3, results3, replace=False, flatten=True)

        df = self.tracker.get(drop_constant_columns=False)
        print(df)
        assert df.shape == (3, 14)

        df = self.tracker.get(drop_constant_columns=True)
        print(df)
        assert df.shape == (3, 2)  # date, time


#        assert self.tracker.shape

if __name__ == "__main__":
    unittest.main()
