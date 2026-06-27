import unittest
import pandas as pd
from track_results import TrackResults

from track_results.track_results import FLATTEN_SEPARATOR


class TestReplaceLogic(unittest.TestCase):
    # ANSI escape codes for colors
    RED = "\033[91m"
    BLUE = "\033[94m"
    ENDC = "\033[0m"

    @classmethod
    def setUpClass(cls):
        """Set up a shared TrackResults instance for all tests in this class."""
        cls.collection = "test_collection_replace"
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

    def test_replace_and_add_duplicate(self):
        """
        Tests the replace logic in the add() method.
        1. Adds an initial record.
        2. Adds a second record with the same parameters and `replace=True`,
           verifying the original record is updated (upsert).
        3. Adds a third record with the same parameters and `replace=False`,
           verifying a new, duplicate record is created.
        """
        failures = []
        success_messages = []

        # Define a set of parameters that will be reused
        # The `platform` info is also part of the matching criteria, but it's
        # automatically captured and will be constant during the test run.
        common_params = {"algorithm": "test_algo", "version": 1}

        # 1. Add the initial record
        self.track.add(parameters=common_params, results={"score": 95})
        df1 = self.track.get(flatten=True, drop_constant_columns=True)
        score_col = f"results{FLATTEN_SEPARATOR}score"

        if len(df1) == 1 and df1[score_col].iloc[0] == 95:
            success_messages.append(
                f"--- Step 1: Add initial record ---\n"
                f"SUCCESS: Correctly added 1 record with {score_col} 95.\n"
                f"DataFrame:\n{df1}"
            )
        else:
            failures.append(
                f"--- Step 1: Add initial record ---\n"
                f"FAILURE: Expected 1 record with score 95.\n"
                f"Found {len(df1)} records.\n"
                f"DataFrame:\n{df1}"
            )

        # 2. Replace the existing record
        self.track.add(
            parameters=common_params,
            results={"score": 99, "status": "final"},
            replace=True,
        )
        df2 = self.track.get(flatten=True, drop_constant_columns=True)
        status_col = f"results{FLATTEN_SEPARATOR}status"

        if len(df2) == 1 and df2[score_col].iloc[0] == 99 and status_col in df2.columns:
            success_messages.append(
                f"--- Step 2: Replace existing record ---\n"
                f"SUCCESS: Correctly replaced record, new score is 99.\n"
                f"DataFrame:\n{df2}"
            )
        else:
            failures.append(
                f"--- Step 2: Replace existing record ---\n"
                f"FAILURE: Expected 1 record to be replaced with score 99.\n"
                f"Found {len(df2)} records.\n"
                f"DataFrame:\n{df2}"
            )

        # 3. Add a new record with the same parameters (no replace)
        self.track.add(parameters=common_params, results={"score": 100}, replace=False)
        df3 = self.track.get(flatten=True, drop_constant_columns=True)

        if len(df3) == 2 and sorted(df3[score_col].tolist()) == [99, 100]:
            success_messages.append(
                f"--- Step 3: Add duplicate record ---\n"
                f"SUCCESS: Correctly added a second record, total is now 2.\n"
                f"DataFrame:\n{df3}"
            )
        else:
            failures.append(
                f"--- Step 3: Add duplicate record ---\n"
                f"FAILURE: Expected 2 records after adding a duplicate.\n"
                f"Found {len(df3)} records.\n"
                f"DataFrame:\n{df3}"
            )

        # 4. Test upsert functionality (add a new record with replace=True)
        new_params = {"algorithm": "new_algo", "version": 1}
        self.track.add(parameters=new_params, results={"score": 80}, replace=True)
        df4 = self.track.get(flatten=True, drop_constant_columns=True)

        if len(df4) == 3 and (df4[score_col] == 80).any():
            success_messages.append(
                f"--- Step 4: Upsert new record ---\n"
                f"SUCCESS: Correctly upserted a new record, total is now 3.\n"
                f"DataFrame:\n{df4}"
            )
        else:
            failures.append(
                f"--- Step 4: Upsert new record ---\n"
                f"FAILURE: Expected 3 records after upserting a new one.\n"
                f"Found {len(df4)} records.\n"
                f"DataFrame:\n{df4}"
            )

        if failures:
            self.fail(
                f"\n--- {self.RED}TEST FAILED{self.ENDC} ---\n\n"
                + "\n\n".join(failures)
            )
        else:
            print(
                f"\n--- {self.BLUE}TEST PASSED: test_replace_and_add_duplicate{self.ENDC} ---\n\n"
                + "\n\n".join(success_messages)
            )


if __name__ == "__main__":
    unittest.main()
