import sys
import os

# Add the project root to the Python path if this file is run directly.
if __name__ == "__main__" and __package__ is None:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    sys.path.insert(0, project_root)

import unittest
import pandas as pd
from track_results.track_results import FLATTEN_SEPARATOR
from tests.base import TestTrackResultsBase, TEST_CONFIGS


class TestFilterAndQuery(TestTrackResultsBase):
    # ANSI escape codes for colors
    RED = "\033[91m"
    BLUE = "\033[94m"
    ENDC = "\033[0m"

    # This flag tells the unittest discovery process to not run this base class directly.
    __test__ = False

    collection_name = "test_collection_query"

    def setUp(self):
        """Ensure the collection is empty and add a standard set of records."""
        super().setUp()
        # If config is not set, it means this is the base class being run directly
        # by a test runner. We should skip the rest of the setup.
        if not self.config:
            self.skipTest("Base class should not be run directly.")

        self.test_prefix = f"[{self.config['name']}]"
        self.records = [
            {
                "params": {"id": 1, "lr": 0.01, "optimizer": "adam"},
                "results": {"score": 95, "loss": 0.1},
            },
            {
                "params": {"id": 2, "lr": 0.02, "optimizer": "adam"},
                "results": {"score": 98, "loss": 0.05},
            },
            {
                "params": {"id": 3, "lr": 0.01, "optimizer": "sgd"},
                "results": {"score": 92, "loss": 0.15},
            },
            {
                "params": {"id": 4, "lr": 0.03, "optimizer": "sgd"},
                "results": {"score": 96, "loss": 0.08},
            },
        ]
        for record in self.records:
            self.track.add(parameters=record["params"], results=record["results"])

    def test_mongodb_filter(self):
        """Tests the MongoDB `filter` on a nested field."""
        test_description = f"{self.test_prefix} MongoDB `filter`"
        df = self.track.get(filter={"parameters.optimizer": "adam"}, flatten=False)
        if len(df) == 2 and all(
            df["parameters"].apply(lambda p: p["optimizer"] == "adam")
        ):
            print(
                f"\n--- {self.BLUE}TEST SUCCESS{self.ENDC}: {test_description} ---\n"
                f"SUCCESS: Correctly filtered for optimizer='adam', found 2 records.\n"
                f"DataFrame:\n{df[['parameters', 'results']]}"
            )
        else:
            self.fail(
                f"\n--- {self.RED}TEST FAILURE{self.ENDC}: {test_description} ---\n"
                f"FAILURE: Expected 2 records with optimizer='adam'.\n"
                f"Found {len(df)} records.\n"
                f"DataFrame:\n{df}"
            )

    def test_pandas_query_flattened(self):
        """Tests the pandas `query` on a flattened DataFrame."""
        test_description = f"{self.test_prefix} Pandas `query` (flattened)"
        score_col = f"results{FLATTEN_SEPARATOR}score"
        id_col = f"parameters{FLATTEN_SEPARATOR}id"
        query_str = f"`{score_col}` > 95"
        df = self.track.get(flatten=True, query=query_str)
        if len(df) == 2 and all(df[score_col] > 95):
            print(
                f"\n--- {self.BLUE}TEST SUCCESS{self.ENDC}: {test_description} ---\n"
                f"SUCCESS: Correctly queried for {score_col} > 95, found 2 records.\n"
                f"DataFrame:\n{df[[id_col, score_col]]}"
            )
        else:
            self.fail(
                f"\n--- {self.RED}TEST FAILURE{self.ENDC}: {test_description} ---\n"
                f"FAILURE: Expected 2 records with {score_col} > 95.\n"
                f"Found {len(df)} records.\n"
                f"DataFrame:\n{df}"
            )

    def test_combined_filter_and_query(self):
        """Tests combining `filter` and `query`."""
        test_description = f"{self.test_prefix} Combined `filter` and `query`"
        id_col = f"parameters{FLATTEN_SEPARATOR}id"
        optimizer_col = f"parameters{FLATTEN_SEPARATOR}optimizer"
        score_col = f"results{FLATTEN_SEPARATOR}score"
        query_str = f"`{score_col}` > 95"

        df = self.track.get(
            filter={"parameters.optimizer": "sgd"},
            flatten=True,
            query=query_str,  # e.g., "`results.score` > 95"
        )
        if len(df) == 1 and df[id_col].iloc[0] == 4:
            print(
                f"\n--- {self.BLUE}TEST SUCCESS{self.ENDC}: {test_description} ---\n"
                f"SUCCESS: Correctly found 1 record matching both criteria.\n"
                f"DataFrame:\n{df[[id_col, optimizer_col, score_col]]}"
            )
        else:
            self.fail(
                f"\n--- {self.RED}TEST FAILURE{self.ENDC}: {test_description} ---\n"
                f"FAILURE: Expected 1 record matching both criteria.\n"
                f"Found {len(df)} records.\n"
                f"DataFrame:\n{df}"
            )

    def test_filter_with_operators(self):
        """Tests the `filter` with MongoDB operators like `$gte`."""
        test_description = f"{self.test_prefix} `filter` with operators"
        df = self.track.get(filter={"results.score": {"$gte": 96}}, flatten=False)
        if len(df) == 2 and all(df["results"].apply(lambda r: r["score"] >= 96)):
            print(
                f"\n--- {self.BLUE}TEST SUCCESS{self.ENDC}: {test_description} ---\n"
                f"SUCCESS: Correctly filtered for score >= 96, found 2 records.\n"
                f"DataFrame:\n{df[['parameters', 'results']]}"
            )
        else:
            self.fail(
                f"\n--- {self.RED}TEST FAILURE{self.ENDC}: {test_description} ---\n"
                f"FAILURE: Expected 2 records with score >= 96.\n"
                f"Found {len(df)} records.\n"
                f"DataFrame:\n{df}"
            )


for config in TEST_CONFIGS:
    class_name = f"TestFilterAndQuery_{config['name']}"
    # Override the inherited __test__ = False from the base class
    attributes = {"config": config, "__test__": True}
    globals()[class_name] = type(class_name, (TestFilterAndQuery,), attributes)


if __name__ == "__main__":
    # Invoke pytest on this file for direct execution.
    import pytest

    sys.exit(pytest.main(["-v", "-s", __file__]))
