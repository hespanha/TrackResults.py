import unittest
import pandas as pd
from track_results import TrackResults

from track_results.track_results import FLATTEN_SEPARATOR


class TestFilterAndQuery(unittest.TestCase):
    # ANSI escape codes for colors
    RED = "\033[91m"
    BLUE = "\033[94m"
    ENDC = "\033[0m"

    @classmethod
    def setUpClass(cls):
        """Set up a shared TrackResults instance for all tests in this class."""
        cls.collection = "test_collection_query"
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
        """Ensure the collection is empty and add a standard set of records."""
        self.track.remove(filter={}, simulate=False, silent=True)
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
        test_description = "MongoDB `filter`"
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
        test_description = "Pandas `query` (flattened)"
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
        test_description = "Combined `filter` and `query`"
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
        test_description = "`filter` with operators"
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


if __name__ == "__main__":
    # You can run all tests in the file like this:
    # python -m unittest tests/test_track_results_query.py
    # Or run a specific test:
    # python -m unittest tests.test_track_results_query.TestFilterAndQuery.test_mongodb_filter
    unittest.main()
