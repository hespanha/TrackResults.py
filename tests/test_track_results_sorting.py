import unittest
import pandas as pd
from track_results import TrackResults
from track_results.track_results import FLATTEN_SEPARATOR
from track_results.track_results import savefig_to_binary

try:
    import matplotlib.pyplot as plt

    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False


@unittest.skipIf(not MATPLOTLIB_AVAILABLE, "matplotlib is not installed")
class TestSortingLogic(unittest.TestCase):
    # ANSI escape codes for colors
    RED = "\033[91m"
    BLUE = "\033[94m"
    ENDC = "\033[0m"

    @classmethod
    def setUpClass(cls):
        """Set up a shared TrackResults instance for all tests in this class."""
        import matplotlib.pyplot as plt

        cls.collection = "test_collection_sorting"
        cls.track = TrackResults(
            uri=None,  # Use Mongita
            collection=cls.collection,
            verbose=False,
        )
        cls.track.drop(simulate=False, silent=True)

        # Create two distinct figure binaries to test sorting on binary data
        fig1, ax1 = plt.subplots()
        ax1.plot([1, 2, 3])
        ax1.set_title("Figure 1")
        cls.figure_binary_1 = savefig_to_binary(fig1)
        plt.close(fig1)

        fig2, ax2 = plt.subplots()
        ax2.plot([4, 5, 6])
        ax2.set_title("Figure 2")
        cls.figure_binary_2 = savefig_to_binary(fig2)
        plt.close(fig2)

    @classmethod
    def tearDownClass(cls):
        """Clean up the database after all tests in this class have run."""
        cls.track.drop(simulate=False, silent=True)

    def setUp(self):
        """Ensure the collection is empty and add a standard set of records."""
        self.track.remove(filter={}, simulate=False, silent=True)
        # Records are added in a non-sorted order to test the sorting logic.
        self.records = [
            {
                "params": {
                    "str_param": "b",
                    "id": 4,
                    "float_param": 2.0,
                    "list_param": [1, 2],
                    "dict_param": {"x": 1},
                    "figure_param": self.figure_binary_1,
                }
            },
            {
                "params": {
                    "str_param": "a",
                    "id": 2,
                    "float_param": 2.0,
                    "list_param": [1, 2],
                    "dict_param": {"x": 1},
                    "figure_param": self.figure_binary_2,
                }
            },
            {
                "params": {
                    "str_param": "a",
                    "id": 1,
                    "float_param": 1.0,
                    "list_param": [1, 2],
                    "dict_param": {"x": 1},
                    "figure_param": self.figure_binary_1,
                }
            },
            {
                "params": {
                    "str_param": "b",
                    "id": 3,
                    "float_param": 1.0,
                    "list_param": [1, 2],
                    "dict_param": {"x": 1},
                    "figure_param": self.figure_binary_2,
                }
            },
        ]
        for record in self.records:
            self.track.add(parameters=record["params"], results={})

    def test_sort_by_parameters_with_complex_types(self):
        """
        Tests that `get(sort_by_params=True)` correctly sorts records based on a mix
        of parameter types (string, float, list, dict).
        """
        test_description = "Sort by complex parameters"
        df = self.track.get(
            flatten=True, sort_by_params=True, drop_constant_columns=True
        )

        # The `get` method with `sort_by_params=True` sorts by all flattened
        # parameter columns alphabetically, then by date.
        # The primary sorting keys will be `parameters.dict_param`, `parameters.float_param`, etc.
        # We expect the final order to be based on these parameter values.
        expected_id_order = [1, 2, 3, 4]
        id_col = f"parameters{FLATTEN_SEPARATOR}id"
        actual_id_order = df[id_col].tolist()

        if actual_id_order == expected_id_order:
            print(
                f"\n--- {self.BLUE}TEST SUCCESS{self.ENDC}: {test_description} ---\n"
                f"SUCCESS: DataFrame is correctly sorted. ID order is {actual_id_order}.\n"
                f"DataFrame:\n{df}"
            )
        else:
            self.fail(
                f"\n--- {self.RED}TEST FAILURE{self.ENDC}: {test_description} ---\n"
                f"FAILURE: Incorrect sort order.\n"
                f"Expected ID order: {expected_id_order}\n"
                f"Actual ID order:   {actual_id_order}\n"
                f"DataFrame:\n{df}"
            )


if __name__ == "__main__":
    # You can run all tests in the file like this:
    # python -m unittest tests/test_track_results_query.py
    # Or run a specific test:
    # python -m unittest tests.test_track_results_query.TestFilterAndQuery.test_mongodb_filter
    unittest.main()
