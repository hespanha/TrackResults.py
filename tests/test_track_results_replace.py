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


class TestReplaceLogic(TestTrackResultsBase):
    # ANSI escape codes for colors
    RED = "\033[91m"
    BLUE = "\033[94m"
    ENDC = "\033[0m"

    # This flag tells the unittest discovery process to not run this base class directly.
    __test__ = False

    collection_name = "test_collection_replace"

    def setUp(self):
        """Set up the test prefix for logging."""
        super().setUp()
        if not self.config:
            self.skipTest("Base class should not be run directly.")
        self.test_prefix = f"[{self.config['name']}]"
        self.common_params = {"algorithm": "test_algo", "version": 1}
        self.score_col = f"results{FLATTEN_SEPARATOR}score"

    def test_initial_add(self):
        """Verify that a single record can be added correctly."""
        test_description = f"{self.test_prefix} Initial Add"
        self.track.add(parameters=self.common_params, results={"score": 95})
        df = self.track.get(flatten=True)

        self.assertEqual(len(df), 1)
        self.assertEqual(df[self.score_col].iloc[0], 95)
        print(
            f"\n--- {self.BLUE}TEST SUCCESS{self.ENDC}: {test_description} ---\n"
            f"DataFrame:\n{df}"
        )

    def test_replace_existing_record(self):
        """Verify that an existing record is replaced when replace=True."""
        test_description = f"{self.test_prefix} Replace Existing"
        # Add initial record
        self.track.add(parameters=self.common_params, results={"score": 95})

        # Replace it
        self.track.add(
            parameters=self.common_params,
            results={"score": 99, "status": "final"},
            replace=True,
        )
        df = self.track.get(flatten=True)
        status_col = f"results{FLATTEN_SEPARATOR}status"

        self.assertEqual(len(df), 1)
        self.assertEqual(df[self.score_col].iloc[0], 99)
        self.assertIn(status_col, df.columns)
        print(
            f"\n--- {self.BLUE}TEST SUCCESS{self.ENDC}: {test_description} ---\n"
            f"DataFrame:\n{df}"
        )

    def test_add_duplicate_record(self):
        """Verify that a duplicate record is added when replace=False."""
        test_description = f"{self.test_prefix} Add Duplicate"
        # Add initial record
        self.track.add(parameters=self.common_params, results={"score": 99})

        # Add a "duplicate" (same params)
        self.track.add(
            parameters=self.common_params, results={"score": 100}, replace=False
        )
        df = self.track.get(flatten=True)

        self.assertEqual(len(df), 2)
        self.assertEqual(sorted(df[self.score_col].tolist()), [99, 100])
        print(
            f"\n--- {self.BLUE}TEST SUCCESS{self.ENDC}: {test_description} ---\n"
            f"DataFrame:\n{df}"
        )

    def test_upsert_new_record(self):
        """Verify that a new record is created if replace=True and no match is found."""
        test_description = f"{self.test_prefix} Upsert New Record"
        # Add one record to ensure the collection is not empty
        self.track.add(parameters=self.common_params, results={"score": 99})

        # Attempt to replace a non-existent record (should upsert)
        new_params = {"algorithm": "new_algo", "version": 1}
        self.track.add(parameters=new_params, results={"score": 80}, replace=True)
        df = self.track.get(flatten=True)

        self.assertEqual(len(df), 2)
        self.assertTrue((df[self.score_col] == 80).any())
        print(
            f"\n--- {self.BLUE}TEST SUCCESS{self.ENDC}: {test_description} ---\n"
            f"DataFrame:\n{df}"
        )


for config in TEST_CONFIGS:
    class_name = f"TestReplaceLogic_{config['name']}"
    # Override the inherited __test__ = False from the base class
    attributes = {"config": config, "__test__": True}
    globals()[class_name] = type(class_name, (TestReplaceLogic,), attributes)


if __name__ == "__main__":
    # Invoke pytest on this file for direct execution.
    import pytest

    sys.exit(pytest.main(["-v", "-s", __file__]))
