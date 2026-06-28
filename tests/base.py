import unittest
from track_results import TrackResults

# --- Test Configuration ---
try:
    # The file my_secrets.py should create a variable `MONGODB_URI` with the URI of the MongoDB.
    import my_secrets

    MONGODB_URI = my_secrets.MONGODB_URI
except ImportError:
    # If the file does not exist, we will only test with mongita.
    MONGODB_URI = None

TEST_CONFIGS = [
    {"uri": None, "name": "Mongita"},
]

if MONGODB_URI:
    TEST_CONFIGS.append({"uri": MONGODB_URI, "name": "MongoDB"})


class TestTrackResultsBase(unittest.TestCase):
    """
    A base test class for TrackResults tests.
    This class is not meant to be run directly but to be inherited by
    test classes that are dynamically generated for each backend (Mongita, MongoDB).
    """

    # These should be set by the child class
    config = {}
    collection_name = "test_collection"

    def setUp(self):
        """Set up a TrackResults instance for the specific backend."""
        self.track = TrackResults(
            uri=self.config.get("uri"),
            collection=self.collection_name,
            verbose=False,
        )
        self.track.drop(simulate=False, silent=True)

    def tearDown(self):
        """Clean up the database after each test."""
        if hasattr(self, "track"):
            self.track.drop(simulate=False, silent=True)
            self.track.close()
