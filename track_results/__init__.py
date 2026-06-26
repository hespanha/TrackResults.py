__all__ = ["interesting_columns", "flatten_dict", "TrackResults"]

import importlib.metadata

# Fetches the version assigned during installation dynamically
__version__ = importlib.metadata.version("track_results")

from .track_results import TrackResults, interesting_columns, flatten_dict
