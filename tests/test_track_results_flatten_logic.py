import unittest
import numpy as np
import pandas as pd
from track_results import TrackResults
from track_results.track_results import FLATTEN_SEPARATOR


class TestFlattenLogic(unittest.TestCase):
    # ANSI escape codes for colors
    RED = "\033[91m"
    BLUE = "\033[94m"
    ENDC = "\033[0m"

    @classmethod
    def setUpClass(cls):
        """Set up a shared TrackResults instance for all tests in this class."""
        cls.collection = "test_collection_flatten"
        cls.track = TrackResults(
            uri=None,  # Use Mongita
            collection=cls.collection,
            verbose=False,
        )
        cls.track.drop(simulate=False, silent=True)

    @classmethod
    def tearDownClass(cls):
        """Clean up the database after all tests in this class have run."""
        cls.track.drop(simulate=False, silent=True)

    def setUp(self):
        """Ensure the collection is empty before each test."""
        self.track.remove(filter={}, simulate=False, silent=True)

        # Define complex, nested records to be used in tests
        self.records = [
            {
                "params": {
                    "id": 1,
                    "nested_dict": {"a": 10, "b": 20},
                    "double_nested_dict": {"aa": {"aaa": 100}, "bb": 20},
                    "list_of_dicts": [{"x": 1}, {"y": 2}],
                },
                "results": {"value": 100, "items": [1, 2, 3]},
            },
            {
                "params": {
                    "id": 2,
                    "nested_dict": {
                        "a": 30,
                        "c": 40,
                    },  # 'b' is missing, 'c' is new
                    "double_nested_dict": {
                        "aa": {"aab": 300},
                        "bb": 40,
                    },  # 'aaa' is missing 'aab' is new
                    "list_of_dicts": [{"x": 3}, {"z": 4}],  # 'y' is missing, 'z' is new
                },
                "results": {"value": 200, "items": [4, 5, 6]},
            },
        ]
        self.expected_flattened_columns = {
            "date",
            f"parameters{FLATTEN_SEPARATOR}id",
            f"parameters{FLATTEN_SEPARATOR}nested_dict{FLATTEN_SEPARATOR}a",
            f"parameters{FLATTEN_SEPARATOR}nested_dict{FLATTEN_SEPARATOR}b",
            f"parameters{FLATTEN_SEPARATOR}nested_dict{FLATTEN_SEPARATOR}c",
            f"parameters{FLATTEN_SEPARATOR}double_nested_dict{FLATTEN_SEPARATOR}aa{FLATTEN_SEPARATOR}aaa",
            f"parameters{FLATTEN_SEPARATOR}double_nested_dict{FLATTEN_SEPARATOR}aa{FLATTEN_SEPARATOR}aab",
            f"parameters{FLATTEN_SEPARATOR}double_nested_dict{FLATTEN_SEPARATOR}bb",
            f"parameters{FLATTEN_SEPARATOR}list_of_dicts",  # Lists of dicts are not flattened by json_normalize
            f"results{FLATTEN_SEPARATOR}value",
            f"results{FLATTEN_SEPARATOR}items",
        }

        self.expected_nonflatten_cols = {"date", "parameters", "results"}

    def _run_test(self, df, expected_cols, test_description):
        """Helper function to validate DataFrame columns and report results."""
        returned_cols = {col for col in df.columns if not col.startswith("platform")}

        if returned_cols == expected_cols:
            print(
                f"\n--- {self.BLUE}TEST SUCCESS{self.ENDC}: {test_description} ---\n"
                f"Verified Columns: {sorted(list(expected_cols))}\n"
                f"Returned DataFrame:\n{df}\n"
            )
        else:
            missing = expected_cols - returned_cols
            extra = returned_cols - expected_cols
            self.fail(
                f"\n--- {self.RED}TEST FAILURE{self.ENDC}: {test_description} ---\n"
                f"Expected Columns: {sorted(list(expected_cols))}\n"
                f"Returned Columns: {sorted(list(returned_cols))}\n"
                f"Missing Columns:  {sorted(list(missing))}\n"
                f"Extra Columns:    {sorted(list(extra))}\n"
                f"Returned DataFrame:\n{df}\n"
            )

    def test_add_nested_get_flattened(self):
        """
        Test adding nested records (flatten=False) and retrieving them as a
        flattened DataFrame (flatten=True).
        """
        for record in self.records:
            self.track.add(
                parameters=record["params"],
                results=record["results"],
            )

        df = self.track.get(flatten=True)
        self._run_test(
            df,
            self.expected_flattened_columns,
            "Add Nested -> Get Flattened",
        )
        self.assertEqual(df.shape[0], 2)
        self.assertEqual(
            df[
                f"parameters{FLATTEN_SEPARATOR}nested_dict{FLATTEN_SEPARATOR}a"
            ].tolist(),
            [10, 30],
        )

    def test_add_nested_get_nested(self):
        """
        Test adding nested records (flatten=False) and retrieving them as a
        nested DataFrame (flatten=False).
        """
        for record in self.records:
            self.track.add(
                parameters=record["params"],
                results=record["results"],
            )

        df = self.track.get(flatten=False)
        self._run_test(
            df,
            self.expected_nonflatten_cols,
            "Add Nested -> Get Nested",
        )
        self.assertEqual(df.shape[0], 2)

        # Verify that the 'parameters' column contains dictionaries
        self.assertTrue(isinstance(df["parameters"].iloc[0], dict))
        self.assertEqual(df["parameters"].iloc[0]["nested_dict"]["a"], 10)


if __name__ == "__main__":
    unittest.main()
