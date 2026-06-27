import unittest
import numpy as np
import pandas as pd
from track_results import TrackResults
from track_results.track_results import (
    savefig_to_binary,
    FLATTEN_SEPARATOR,
)
from bson.binary import Binary

try:
    import matplotlib.pyplot as plt

    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False


class TestDropConstantColumns(unittest.TestCase):
    # ANSI escape codes for colors
    RED = "\033[91m"
    BLUE = "\033[94m"
    ENDC = "\033[0m"

    @classmethod
    def setUpClass(cls):
        """Set up a shared TrackResults instance for all tests in this class."""
        cls.collection = "test_collection_drop"
        cls.track = TrackResults(
            uri=None,  # Use Mongita
            collection=cls.collection,
            verbose=False,
        )
        cls.track.drop(
            simulate=False, silent=True
        )  # Ensure the collection is empty before starting

    @classmethod
    def tearDownClass(cls):
        """Clean up the database after all tests in this class have run."""
        cls.track.drop(simulate=False, silent=True)

    def setUp(self):
        """Ensure the collection is empty before each test."""
        self.track.remove(filter={}, simulate=False, silent=True)

    def _run_test(
        self, records, dropped_cols, kept_cols, test_description, flatten=True
    ):
        """Helper function to run a standard test procedure."""
        for record in records:
            self.track.add(
                parameters=record.get("params", {}),
                results=record.get("results", {}),
            )

        # Get the DataFrame both with and without dropping columns
        df_full = self.track.get(flatten=flatten, drop_constant_columns=False)
        df_dropped = self.track.get(flatten=flatten, drop_constant_columns=True)

        returned_cols = set(df_dropped.columns)

        error_messages = []

        # Assert that expected columns are kept
        missing_kept_cols = set(kept_cols) - returned_cols
        if len(missing_kept_cols) > 0:
            error_messages.append(
                f"--- {self.RED}TEST FAILURE{self.ENDC}: Some expected columns were dropped ---\n"
                f"Expected to keep: {kept_cols}\n"
                f"Actually kept:    {list(returned_cols)}\n"
                f"Missing columns:  {list(missing_kept_cols)}"
            )

        # Assert that constant columns are dropped
        found_dropped_cols = set(dropped_cols).intersection(returned_cols)
        if len(found_dropped_cols) > 0:
            error_messages.append(
                f"--- {self.RED}TEST FAILURE{self.ENDC}: Some constant columns were not dropped ---\n"
                f"Expected to drop: {dropped_cols}\n"
                f"Actually kept:    {list(returned_cols)}\n"
                f"Kept but should have been dropped: {list(found_dropped_cols)}"
            )

        if error_messages:
            full_error_message = "\n\n".join(error_messages)
            self.fail(
                f"\n{full_error_message}\n\n"
                f"--- DataFrame Details ---\n"
                f"Original DataFrame:\n{df_full}\n\n"
                f"Returned DataFrame:\n{df_dropped}\n"
            )
        else:
            # On success, print the summary
            print(
                f"\n--- {self.BLUE}TEST SUCCESS{self.ENDC}: {test_description} ---\n"
                f"Expected to drop: {dropped_cols}\n"
                f"Expected to keep: {kept_cols}\n"
                f"Original DataFrame:\n{df_full}\n"
                f"Returned DataFrame:\n{df_dropped}\n"
            )

    def test_drop_constant_columns_none(self):
        """Verify that columns with only `None` are dropped."""
        records = [
            {"params": {"id": 1}, "results": {"const_none": None, "mixed_none": None}},
            {
                "params": {"id": 2},
                "results": {"const_none": None, "mixed_none": "not none"},
            },
        ]
        self._run_test(
            records,
            dropped_cols=[f"results{FLATTEN_SEPARATOR}const_none"],
            kept_cols=[
                f"parameters{FLATTEN_SEPARATOR}id",
                f"results{FLATTEN_SEPARATOR}mixed_none",
            ],
            test_description="Columns with only `None` are dropped",
        )

    def test_drop_constant_columns_nan(self):
        """Verify that columns with only `np.nan` are dropped."""
        records = [
            {
                "params": {"id": 1},
                "results": {"const_nan": np.nan, "mixed_nan": np.nan},
            },
            {"params": {"id": 2}, "results": {"const_nan": np.nan, "mixed_nan": 3.14}},
        ]
        self._run_test(
            records,
            dropped_cols=[f"results{FLATTEN_SEPARATOR}const_nan"],
            kept_cols=[
                f"parameters{FLATTEN_SEPARATOR}id",
                f"results{FLATTEN_SEPARATOR}mixed_nan",
            ],
            test_description="Columns with only `np.nan` are dropped",
        )

    def test_drop_constant_columns_list(self):
        """Verify that columns with identical lists are dropped."""
        records = [
            {
                "params": {"id": 1},
                "results": {"const_list": [1, 2], "mixed_list": [1, 2]},
            },
            {
                "params": {"id": 2},
                "results": {"const_list": [1, 2], "mixed_list": [1, 2, 3]},
            },
        ]
        self._run_test(
            records,
            dropped_cols=[f"results{FLATTEN_SEPARATOR}const_list"],
            kept_cols=[
                f"parameters{FLATTEN_SEPARATOR}id",
                f"results{FLATTEN_SEPARATOR}mixed_list",
            ],
            test_description="Columns with identical lists are dropped",
        )

    def test_drop_constant_columns_dict_no_flatten(self):
        """Verify that columns with identical dicts are dropped."""
        records = [
            {
                "params": {"id": 1},
                "results": {"const_dict": {"a": 1}, "mixed_dict": {"a": 1}},
            },
            {
                "params": {"id": 2},
                "results": {"const_dict": {"a": 1}, "mixed_dict": {"b": 2}},
            },
        ]
        self._run_test(
            records,
            dropped_cols=[],  # No columns are dropped as the 'results' dicts differ
            kept_cols=["date", "parameters", "results"],
            test_description="Columns with identical dicts are dropped (no flatten)",
            flatten=False,
        )

    def test_drop_constant_columns_dict_flatten(self):
        """Verify that columns with identical dicts are dropped when flattened."""
        records = [
            {
                "params": {"id": 1},
                "results": {"const_dict": {"a": 1}, "mixed_dict": {"a": 1}},
            },
            {
                "params": {"id": 2},
                "results": {"const_dict": {"a": 1}, "mixed_dict": {"b": 2}},
            },
        ]
        self._run_test(
            records,
            dropped_cols=[f"results{FLATTEN_SEPARATOR}const_dict{FLATTEN_SEPARATOR}a"],
            kept_cols=[
                f"parameters{FLATTEN_SEPARATOR}id",
                f"results{FLATTEN_SEPARATOR}mixed_dict{FLATTEN_SEPARATOR}a",
                f"results{FLATTEN_SEPARATOR}mixed_dict{FLATTEN_SEPARATOR}b",
            ],
            test_description="Columns with identical dicts are dropped (flatten)",
            flatten=True,
        )

    def test_drop_constant_columns_binary(self):
        """Verify that columns with identical Binary fields are dropped."""
        # Create some distinct binary objects
        binary1 = Binary(b"\x01\x02\x03")
        binary2 = Binary(b"\x04\x05\x06")

        records = [
            {
                "params": {"id": 1},
                "results": {"const_binary": binary1, "mixed_binary": binary1},
            },
            {
                "params": {"id": 2},
                "results": {"const_binary": binary1, "mixed_binary": binary2},
            },
        ]
        self._run_test(
            records,
            dropped_cols=[f"results{FLATTEN_SEPARATOR}const_binary"],
            kept_cols=[
                f"parameters{FLATTEN_SEPARATOR}id",
                f"results{FLATTEN_SEPARATOR}mixed_binary",
            ],
            test_description="Columns with identical Binary objects are dropped",
        )

    @unittest.skipIf(not MATPLOTLIB_AVAILABLE, "matplotlib is not installed")
    def test_drop_constant_columns_matplotlib_binary(self):
        """Verify that columns with identical matplotlib figures (as binary) are dropped."""
        import matplotlib.pyplot as plt

        # Create some distinct binary objects from matplotlib figures
        fig1, ax1 = plt.subplots()
        ax1.plot([1, 2, 3], label="A")
        binary1 = savefig_to_binary(fig1)
        plt.close(fig1)

        # Create a second, identical figure to test for equality
        fig1_copy, ax1_copy = plt.subplots()
        ax1_copy.plot([1, 2, 3], label="A")
        binary1_copy = savefig_to_binary(fig1_copy)
        plt.close(fig1_copy)

        # Create a different figure
        fig2, ax2 = plt.subplots()
        ax2.plot([4, 5, 6], label="B")
        binary2 = savefig_to_binary(fig2)
        plt.close(fig2)

        records = [
            {
                "params": {"id": 1},
                "results": {"const_binary": binary1, "mixed_binary": binary1_copy},
            },
            {
                "params": {"id": 2},
                "results": {"const_binary": binary1, "mixed_binary": binary2},
            },
        ]
        self._run_test(
            records,
            dropped_cols=[f"results{FLATTEN_SEPARATOR}const_binary"],
            kept_cols=[
                f"parameters{FLATTEN_SEPARATOR}id",
                f"results{FLATTEN_SEPARATOR}mixed_binary",
            ],
            test_description="Columns with identical matplotlib figures are dropped",
        )


if __name__ == "__main__":
    unittest.main()
