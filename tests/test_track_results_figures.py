import unittest
import tempfile
import shutil
import os
import webbrowser
import copy
import sys
from pathlib import Path
import numpy as np

from track_results.track_results import (
    TrackResults,
    savefig_to_binary,
    binary_to_pdf,
    savefig_pickle2binary,
    FLATTEN_SEPARATOR,
    pickle2binary_to_fig,
)

try:
    # the file my_secrets.py should create a variable `MONGODB_URI` with the URI of the MongoDB
    import my_secrets

    MONGODB_URI = my_secrets.MONGODB_URI
except:
    # if the file does not exist, will default to a mongita database
    MONGODB_URI = None


try:
    import matplotlib.pyplot as plt

    MATPLOTLIB_AVAILABLE = True
except ImportError:
    MATPLOTLIB_AVAILABLE = False


@unittest.skipIf(not MATPLOTLIB_AVAILABLE, "matplotlib is not installed")
class TestTrackResultsFigures(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        """Set up a temporary directory and a shared TrackResults instance for all tests."""
        cls.test_dir = tempfile.mkdtemp()
        cls.output_pdf_path = os.path.join(cls.test_dir, "output_figure.pdf")
        # Use mongita for local testing by not providing a URI
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

    @classmethod
    def tearDownClass(cls):
        """Show all figures and then clean up the temporary directory."""
        import matplotlib.pyplot as plt

        # Only show plots for interactive inspection when not running under pytest.
        # `pytest` imports itself, so we can check sys.modules.
        if "pytest" not in sys.modules:
            print("\nDisplaying all test figures. Close all plot windows to finish.")
            plt.show()
        else:
            print("\nPytest is running, skipping interactive plt.show().")

        # Clean up after visual inspection
        cls.tracker.drop(simulate=False, silent=True)
        shutil.rmtree(cls.test_dir)

    def setUp(self):
        """Set up a temporary directory and a TrackResults instance for testing."""
        # Use mongita for local testing by not providing a URI
        self.tracker.remove(
            filter={}, simulate=False, silent=True
        )  # Ensure a clean slate for each test

    def test_figure_save_and_load_pipeline(self):
        """
        Tests the full pipeline:
        1. Create a matplotlib figure.
        2. Convert it to binary.
        3. Add it to the tracker.
        4. Retrieve it from the tracker.
        5. Decode the binary back to a PDF file.
        """
        import matplotlib.pyplot as plt

        # 1. Create a matplotlib figure
        fig, ax = plt.subplots()

        # Sine function
        x = np.linspace(0, 10, 100)
        y = np.sin(x)
        ax.plot(x, y, label="sin(x)")

        # Scatter plot
        x_scatter = np.random.rand(20) * 10
        y_scatter = np.sin(x_scatter) + np.random.randn(20) * 0.2
        ax.scatter(x_scatter, y_scatter, color="red", label="scatter points")

        # Transparent band
        ax.fill_between(
            x, y - 0.2, y + 0.2, color="gray", alpha=0.3, label="confidence band"
        )

        # Add grid, title, and labels
        ax.grid(True)
        ax.set_title("Figure saved ad .pdf")
        ax.set_xlabel("x-axis")
        ax.set_ylabel("y-axis")
        ax.legend()

        # 2. Convert it to binary
        figure_binary = savefig_to_binary(fig)

        # 3. Add it to the tracker
        parameters = {"test_name": "figure_pipeline"}
        results = {"my_figure": figure_binary}
        self.tracker.add(parameters=parameters, results=results)

        # 4. Retrieve it from the tracker
        df = self.tracker.get(flatten=True, drop_constant_columns=False)
        print(df)
        self.assertEqual(len(df), 1)
        self.assertIn(f"results{FLATTEN_SEPARATOR}my_figure", df.columns)

        retrieved_binary = df[f"results{FLATTEN_SEPARATOR}my_figure"].iloc[0]

        # 5. Decode the binary back to a PDF file
        binary_to_pdf(retrieved_binary, self.output_pdf_path)

        # Verify that the PDF file was created
        self.assertTrue(os.path.exists(self.output_pdf_path))

        # To visually inspect the saved PDF file, this will open the PDF
        # in your system's default viewer.
        pdf_uri = Path(self.output_pdf_path).as_uri()
        print(f"\nOpening saved PDF file: {pdf_uri}")
        webbrowser.open_new(pdf_uri)

    def test_figure_pickle_and_modify_pipeline(self):
        """
        Tests pickling a figure, saving it, loading it, and modifying it.
        """
        import matplotlib.pyplot as plt

        # 1. Create an initial matplotlib figure
        fig, ax = plt.subplots(figsize=(8, 6))

        # Cosine function
        x = np.linspace(0, 10, 100)
        y = np.cos(x)
        ax.plot(x, y, label="cos(x)", color="blue", linestyle="--")

        # Scatter plot for the cosine
        x_scatter = np.random.rand(25) * 10
        y_scatter = np.cos(x_scatter) + np.random.randn(25) * 0.25
        ax.scatter(x_scatter, y_scatter, color="green", label="noisy points")

        # Add a transparent band
        ax.fill_between(
            x, y - 0.25, y + 0.25, color="cyan", alpha=0.2, label="confidence"
        )

        # Add grid, title, and labels
        ax.grid(True, linestyle=":", alpha=0.6)
        ax.set_title("Original Figure (saved as pickle)")
        ax.set_xlabel("Angle [rad]")
        ax.set_ylabel("Value")
        ax.legend()
        fig.canvas.draw()  # Ensure the figure is rendered before pickling

        # 2. Serialize the figure object using pickle
        pickled_figure = savefig_pickle2binary(fig)

        # 3. Add it to the tracker
        parameters = {"test_name": "pickle_pipeline"}
        results = {"pickled_figure": pickled_figure}
        self.tracker.add(parameters=parameters, results=results)

        # 4. Retrieve it from the tracker
        df = self.tracker.get(flatten=True, drop_constant_columns=False)
        self.assertEqual(len(df), 1)
        retrieved_binary = df[f"results{FLATTEN_SEPARATOR}pickled_figure"].iloc[0]

        # 5. Deserialize the figure back into an object
        loaded_fig = pickle2binary_to_fig(retrieved_binary)

        # 6. Modify the loaded figure
        ax_loaded = loaded_fig.axes[0]
        ax_loaded.set_title("Modified Figure (from Pickle)")
        # Change line color and style
        ax_loaded.lines[0].set_color("magenta")
        ax_loaded.lines[0].set_linestyle("-")
        # Change scatter plot color
        ax_loaded.collections[0].set_facecolor("red")
        ax_loaded.legend(["cos(x) modified", "noisy points modified", "confidence"])

    def test_get_figure_methods(self):
        """
        Tests the `get_figure_as_pdf` and `get_figure_object` methods,
        including nested field access.
        """
        import matplotlib.pyplot as plt
        from matplotlib.figure import Figure

        # --- 1. Prepare PDF-based figure ---
        fig_pdf, ax_pdf = plt.subplots()
        ax_pdf.plot([1, 2, 3], label="PDF")
        ax_pdf.set_title("PDF Figure")
        pdf_binary = savefig_to_binary(fig_pdf)
        plt.close(fig_pdf)

        # --- 2. Prepare pickled figure ---
        fig_pickle, ax_pickle = plt.subplots()
        ax_pickle.plot([4, 5, 6], label="Pickle")
        ax_pickle.set_title("Pickle Figure")
        pickle_binary = savefig_pickle2binary(fig_pickle)
        # Don't close this one, we'll compare it to the loaded version

        # --- 3. Add records to the database ---
        # Record 1: Figures at the top level of 'results'
        self.tracker.add(
            parameters={"test_name": "get_figure_top_level"},
            results={"pdf_fig": pdf_binary, "pickle_fig": pickle_binary},
        )
        # Record 2: Figures in a nested dictionary
        self.tracker.add(
            parameters={"test_name": "get_figure_nested"},
            results={"figures": {"pdf_fig": pdf_binary, "pickle_fig": pickle_binary}},
        )

        # --- 4. Retrieve records to get their _ids ---
        df = self.tracker.get(flatten=False)
        self.assertEqual(len(df), 2)
        top_level_record = df[
            df["parameters"].apply(lambda p: p["test_name"] == "get_figure_top_level")
        ]
        nested_record = df[
            df["parameters"].apply(lambda p: p["test_name"] == "get_figure_nested")
        ]
        top_level_id = top_level_record.index[0]
        nested_id = nested_record.index[0]

        # --- 5. Test get_figure_as_pdf ---
        pdf_path_1 = os.path.join(self.test_dir, "retrieved_top_level.pdf")
        pdf_path_2 = os.path.join(self.test_dir, "retrieved_nested.pdf")

        # Test with top-level field and string _id
        self.tracker.get_figure_as_pdf(
            _id=str(top_level_id),
            field_name="results.pdf_fig",
            output_filename=pdf_path_1,
        )
        self.assertTrue(os.path.exists(pdf_path_1))

        # Test with nested field and ObjectId _id
        self.tracker.get_figure_as_pdf(
            _id=nested_id,
            field_name="results.figures.pdf_fig",
            output_filename=pdf_path_2,
        )
        self.assertTrue(os.path.exists(pdf_path_2))

        # --- 6. Test get_figure_object ---
        # Test with top-level field
        loaded_fig_1 = self.tracker.get_figure_object(
            _id=top_level_id, field_name="results.pickle_fig"
        )
        self.assertIsInstance(loaded_fig_1, Figure)
        self.assertEqual(loaded_fig_1.axes[0].get_title(), "Pickle Figure")

        # Test with nested field
        loaded_fig_2 = self.tracker.get_figure_object(
            _id=nested_id, field_name="results.figures.pickle_fig"
        )
        self.assertIsInstance(loaded_fig_2, Figure)
        # Verify content is the same as the original
        self.assertEqual(loaded_fig_2.axes[0].lines[0].get_ydata().tolist(), [4, 5, 6])


if __name__ == "__main__":
    unittest.main()
