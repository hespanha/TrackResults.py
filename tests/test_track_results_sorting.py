import sys
import os

# Add the project root to the Python path if this file is run directly.
if __name__ == "__main__" and __package__ is None:
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    sys.path.insert(0, project_root)

import unittest
import pandas as pd
from track_results.track_results import FLATTEN_SEPARATOR
from track_results.track_results import savefig_to_binary
from tests.base import TestTrackResultsBase, TEST_CONFIGS

try:
    import matplotlib.pyplot as plt

    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False


@unittest.skipIf(not MATPLOTLIB_AVAILABLE, "matplotlib is not installed")
class TestSortingLogic(TestTrackResultsBase):
    # ANSI escape codes for colors
    RED = "\033[91m"
    BLUE = "\033[94m"
    ENDC = "\033[0m"

    # This flag tells the unittest discovery process to not run this base class directly.
    __test__ = False

    collection_name = "test_collection_sorting"

    @classmethod
    def setUpClass(cls):
        import matplotlib.pyplot as plt

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

    def setUp(self):
        """Ensure the collection is empty and add a standard set of records."""
        super().setUp()
        if not self.config:
            self.skipTest("Base class should not be run directly.")
        self.test_prefix = f"[{self.config['name']}]"
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
        test_description = f"{self.test_prefix} Sort by complex parameters"
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


for config in TEST_CONFIGS:
    class_name = f"TestSortingLogic_{config['name']}"
    # Override the inherited __test__ = False from the base class
    attributes = {"config": config, "__test__": True}
    globals()[class_name] = type(class_name, (TestSortingLogic,), attributes)


if __name__ == "__main__":
    # Invoke pytest on this file for direct execution.
    import pytest

    sys.exit(pytest.main(["-v", "-s", __file__]))
