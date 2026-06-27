__all__ = [
    "interesting_columns",
    "TrackResults",
    "savefig_to_binary",
    "binary_to_pdf",
    "savefig_pickle2binary",
    "pickle2binary_to_fig",
]

import importlib.metadata

# Fetches the version assigned during installation dynamically
__version__ = importlib.metadata.version("track_results")

from .track_results import (
    TrackResults,
    interesting_columns,
    savefig_to_binary,
    binary_to_pdf,
    savefig_pickle2binary,
    pickle2binary_to_fig,
)
