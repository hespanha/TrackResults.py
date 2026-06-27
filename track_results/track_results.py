"""
Package to track the quality of results obtained different algorithms

Example of usage:

```python
track=TrackResults(filename)   # file where results will be saved (supports fsspec)

# add results to file
track.add(parameters,results)

# get pandas table with summary of results
df=track.query({class:,game:},last_number:int,last_time:time.period|time.duration,columns=())
```

"""

from __future__ import annotations
from typing import Any, cast

import io
import pickle
from bson.binary import Binary
from bson.objectid import ObjectId
from bson.errors import InvalidId


import pydantic

import platform
import uuid
import numpy as np
import pandas as pd
import datetime
import pprint
import json

#######################
## Dictionary utilities
#######################

FLATTEN_SEPARATOR = "."

TYPES_TO_KEEP_AS_IS = (
    type(None),
    bool,
    int,
    float,
    bytes,
    str,
    pd.Timestamp,
    datetime.datetime,
    uuid.UUID,
    Binary,
)

# not natively supported by pymongo, but supported when converted to list
TYPE_CONVERT_TO_LIST = (
    tuple,
    set,
)


def _sanitize_for_db(data: dict[Any, Any]) -> dict[str, Any]:
    """
    Recursively sanitizes data to ensure it's compatible with MongoDB.
    - Converts tuples, sets, and numpy arrays to lists.
    - Converts other non-standard types to strings.

    Args:
        data (Any): The data to sanitize (e.g., a dictionary, list, or primitive).

    Returns:
        Any: The sanitized data.
    """
    if isinstance(data, dict):
        return {str(k): _sanitize_for_db(v) for k, v in data.items()}

    if isinstance(data, list):
        return [_sanitize_for_db(v) for v in data]

    # Convert types that are not natively supported by MongoDB/mongita
    if isinstance(data, TYPE_CONVERT_TO_LIST):
        return [_sanitize_for_db(v) for v in data]
    if isinstance(data, np.ndarray):
        return data.tolist()

    # Keep basic, compatible types as they are
    if isinstance(data, TYPES_TO_KEEP_AS_IS):
        return data

    # If all else fails, convert to string to ensure sortability and storage
    return str(data)


#######################
## DataFrame utilities
#######################


def interesting_column(df: pd.DataFrame, col: str) -> bool:
    series = df[col]
    # For simple numeric, boolean, or datetime types, nunique is efficient and correct.
    # 'b' for bool, 'i' for int, 'u' for uint, 'f' for float, 'M' for datetime.
    if series.dtype.kind in "biufM":
        return series.nunique(dropna=False) > 1

    # For object types that can contain anything (lists, dicts, Binary, None, etc.),
    # we need a more robust way to check for uniqueness.
    def to_comparable(x):
        """Converts values to a comparable/hashable representation."""
        if isinstance(x, (list, dict)):
            return json.dumps(x, sort_keys=True)
        # For other types like Binary, numbers, None, they are already hashable.
        return x

    try:
        # Apply the conversion and then check for uniqueness.
        # Using a set is efficient for this.
        unique_values = {to_comparable(v) for v in series}
        return len(unique_values) > 1
    except TypeError:
        # If any object is not hashable and not JSON-serializable,
        # we play it safe and keep the column.
        return True


def interesting_columns(
    df: pd.DataFrame,
    cols2search: list[str] | pd.Index | None = None,
    keep: list[str] = [],
) -> pd.DataFrame:
    if cols2search is None:
        cols2search = list(df.columns)
    # print(cols2search)
    if df.shape[0] > 1:
        # multiple rows: drop constant columns (only works for hashable types)
        interesting_cols = [
            col for col in cols2search if col in keep or interesting_column(df, col)
        ]
    else:
        # only 1 row: drop NaNs
        interesting_cols = [
            col
            for col in cols2search
            if not isinstance(df.iloc[0][col], float)
            or (isinstance(df.iloc[0][col], float) and not np.isnan(df.iloc[0][col]))
            or col in keep
        ]
    if len(interesting_cols) == 0:
        raise ValueError("All columns dropped")
    return df[interesting_cols]


#######################
## Formatting utilities
#######################


def savefig_to_binary(fig, format="pdf") -> Binary:
    """Saves a matplotlib figure to a binary PDF format for storage.

    Args:
        fig (Figure): The matplotlib figure to be saved.

    Returns:
        Binary: A binary representation of the figure in PDF format.
    """
    try:
        from matplotlib.figure import Figure
    except ImportError:
        raise ImportError(
            "To use this function, please install matplotlib. "
            "You can install it with: pip install track_results[matplotlib]"
        )

    assert isinstance(fig, Figure), "Input must be a matplotlib Figure."
    pdf_buffer = io.BytesIO()
    fig.savefig(pdf_buffer, format=format, bbox_inches="tight")
    pdf_buffer.seek(0)
    binary_data = Binary(pdf_buffer.read())
    print(f"savefig_to_binary: created binary container with {len(binary_data)} bytes")
    return binary_data


def binary_to_pdf(binary_data: Binary, filename: str):
    """Decodes a binary object and saves it to a file.

    This is useful to recover a PDF file stored using `save_figure_to_binary`.

    Args:
        binary_data (Binary): The binary data to decode.
        filename (str): The path to save the file. The file extension should
            match the original format (e.g., '.pdf').
    """
    if not isinstance(binary_data, Binary):
        raise TypeError("Input must be a bson.binary.Binary object.")

    with open(filename, "wb") as f:
        f.write(binary_data)
    print(f"Figure saved to {filename}")


def savefig_pickle2binary(fig) -> Binary:
    """Serializes a matplotlib figure object using pickle for storage.

    This allows the figure to be reloaded as a Python object for further modification.

    Warning:
        Unpickling data from an untrusted source is a security risk. Only use
        this function on data that your application has generated.

    Args:
        fig (Figure): The matplotlib figure to be serialized.

    Returns:
        Binary: A binary representation of the pickled figure object.
    """
    try:
        from matplotlib.figure import Figure
    except ImportError:
        raise ImportError(
            "To use this function, please install matplotlib. "
            "You can install it with: pip install track_results[matplotlib]"
        )
    if not isinstance(fig, Figure):
        raise TypeError("Input must be a matplotlib.figure.Figure object.")

    pickled_fig = pickle.dumps(fig)
    binary_data = Binary(pickled_fig)
    print(
        f"savefig_pickle2binary: created binary container with {len(binary_data)} bytes"
    )
    return binary_data


def pickle2binary_to_fig(binary_data: Binary):
    """Deserializes a binary object into a matplotlib figure.

    Args:
        binary_data (Binary): The binary data containing the pickled figure.

    Returns:
        Figure: The deserialized matplotlib figure object.
    """
    if not isinstance(binary_data, Binary):
        raise TypeError("Input must be a bson.binary.Binary object.")

    try:
        from matplotlib.figure import Figure
    except ImportError:
        raise ImportError(
            "To use this function, please install matplotlib. "
            "You can install it with: pip install track_results[matplotlib]"
        )
    from matplotlib.figure import Figure

    fig = pickle.loads(binary_data)

    if not isinstance(fig, Figure):
        raise TypeError("Deserialized object is not a matplotlib.figure.Figure.")

    return fig


#########################
## Class to track results
#########################

import pymongo
import pymongo.database
import pymongo.collection

import mongita
from mongita.database import Database
from mongita.collection import Collection


class TrackResults:
    """
    Class to track and save results of experiments to a Mongita database
    """

    __slots__ = [
        "database",
        "collection",
        "columns",
    ]

    database: pymongo.database.Database | Database
    collection: pymongo.collection.Collection | Collection
    columns: dict[str, str] | None

    def __init__(
        self,
        *,
        uri: str | None = None,
        database: str = "track_results",
        collection: str,
        verbose=True,
    ) -> None:
        """
        Initializes the TrackResults instance.

        Args:
            filename (str): The path to the file where results will be saved.
        """
        if uri is None:
            print("TrackResultsTrackResultsMongoDB: using MongitaClientDisk")
            client = mongita.MongitaClientDisk()
        else:
            print("TrackResultsTrackResultsMongoDB: using MongoClient")
            client = pymongo.MongoClient(uri)
        self.database = client[database]

        self.collection = self.database[collection]

        self.columns = None

        if verbose:
            collection_names = self.database.list_collection_names()
            print(
                f'Selecting collection="{collection}" (with {len(self)} records) from:'
            )
            for i, name in enumerate(collection_names):
                print(f"  {i:3}: {name}")

    def __len__(self):
        return self.collection.count_documents({})

    def drop(self, simulate: bool = True, silent: bool = False):
        count = self.collection.count_documents({})
        if simulate:
            if not silent:
                print(
                    f"TrackResultsMongoDB.drop: simulated drop of collection={self.collection.name} with {count} documents"
                )
        else:
            self.database.drop_collection(self.collection.name)
            if not silent:
                print(
                    f"TrackResultsMongoDB.drop: drop of collection={self.collection.name} with {count} documents"
                )

    def add(
        self,
        parameters: dict[str, Any] | pydantic.BaseModel,
        results: dict[str, Any] | pydantic.BaseModel,
        replace: bool = False,
    ) -> None:
        """
        Adds a new result record to the tracking file.

        Args:
            parameters (dict[str, Any]): The parameters used for the experiment.
            results (dict[str, Any]): The results obtained from the experiment.
        """

        # convert pydantic.BaseModel to regular dict supported by pymongo and mongita
        if isinstance(parameters, pydantic.BaseModel):
            parameters = parameters.model_dump()
        if isinstance(results, pydantic.BaseModel):
            results = results.model_dump()

        platform_dict = {
            "node": platform.node(),
            # convert to list to be compatible with pymongo and mongita
            "architecture": list(platform.architecture()),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "system": platform.system(),
            "release": platform.release(),
            "version": platform.version(),
            "python_version": platform.python_version(),
            "python_implementation": platform.python_implementation(),
        }

        fields2match: dict[str, Any] = _sanitize_for_db(
            {
                "parameters": parameters,
                "platform": platform_dict,
            }
        )
        full_record: dict[str, Any] = _sanitize_for_db(
            {
                "date": pd.Timestamp.now(),
                "platform": platform_dict,
                "parameters": parameters,
                "results": results,
            }
        )

        # pprint.pprint(record, indent=3)

        if replace:
            rc = self.collection.replace_one(
                filter=fields2match,
                replacement=full_record,
                upsert=True,  # add if not there
            )
            # print("fields_to_match:", fields_to_match)
            # print("rc:", rc)
            # data: list[dict[str, Any]] = [doc for doc in self.collection.find(filter)]
            # print(data)

            if rc.matched_count > 0:
                assert rc.upserted_id is None
                print(
                    f"TrackResultsMongoDB.add(replace=True): replaced record with matched_count={rc.matched_count}"
                )
            else:
                assert rc.upserted_id is not None
                print(
                    f"TrackResultsMongoDB.add(replace=True): added record with upserted_id={rc.upserted_id}"
                )
        else:
            rc = self.collection.insert_one(full_record)
            # print("rc:", rc)
            print(
                f"TrackResultsMongoDB.add(replace=False): inserted record with id={rc.inserted_id}"
            )

    def _get_nested_field(self, _id: Any, field_name: str) -> Any:
        """
        Internal helper to retrieve a potentially nested field from a specific document.

        Args:
            _id (Any): The _id of the document to retrieve.
            field_name (str): The name of the field to retrieve. Can use dot notation
                              for nested fields (e.g., 'results.my_figure').

        Returns:
            Any: The value of the requested field.

        Raises:
            ValueError: If no document with the given _id is found.
            KeyError: If the field is not found in the document.
        """

        # Projection ensures we only retrieve the necessary field, which is efficient.
        processed_id = _id
        if isinstance(_id, str):
            try:
                # Attempt to convert string to ObjectId, as this is a common use case.
                processed_id = ObjectId(_id)
            except InvalidId:
                # If conversion fails, assume the _id was intended to be a string.
                pass

        # Conditionally apply projection.
        if isinstance(self.collection, pymongo.collection.Collection):
            # For real MongoDB, use projection for efficiency.
            doc = self.collection.find_one(
                {"_id": processed_id}, projection={field_name: 1}
            )
        else:
            # Mongita currently does not support projections.
            # This is acceptable as it's a fast lookup by _id.
            doc = self.collection.find_one({"_id": processed_id})

        if doc is None:
            raise ValueError(f"No document found with _id: {_id}")

        # Traverse the document using the dot-separated field name
        value = doc
        for key in field_name.split("."):
            if isinstance(value, dict):
                value = value.get(key)
                if value is None:
                    raise KeyError(
                        f"Field part '{key}' not found in the path '{field_name}' for document with _id: {_id}"
                    )
            else:
                raise KeyError(
                    f"Cannot access sub-field '{key}'; '{field_name}' path is invalid for document with _id: {_id}"
                )
        return value

    def get_figure_as_pdf(
        self, _id: Any, field_name: str, output_filename: str
    ) -> None:
        """
        Retrieves a figure stored as binary PDF data and saves it to a file.

        Args:
            _id (Any): The _id of the document containing the figure.
            field_name (str): The field name where the figure's binary data is stored
                              (e.g., 'results.my_figure').
            output_filename (str): The path where the output PDF file will be saved.
        """
        binary_data = self._get_nested_field(_id, field_name)

        if not isinstance(binary_data, Binary):
            raise TypeError(
                f"The data in field '{field_name}' is not of type bson.binary.Binary. "
                f"Found type: {type(binary_data).__name__}"
            )

        binary_to_pdf(binary_data, output_filename)

    def get_figure_object(self, _id: Any, field_name: str):
        """
        Retrieves a pickled matplotlib figure and returns it as a Figure object.

        Args:
            _id (Any): The _id of the document containing the figure.
            field_name (str): The field name where the pickled figure is stored
                              (e.g., 'results.pickled_figure').

        Returns:
            matplotlib.figure.Figure: The deserialized matplotlib figure object.
        """
        binary_data = self._get_nested_field(_id, field_name)

        # The pickle2binary_to_fig function already handles the type check for Binary
        return pickle2binary_to_fig(binary_data)

    def __str__(self):
        data = list(self.collection.find({}))
        return str(data)

    def get(
        self,
        *,
        filter: dict[str, Any] = {},
        exclude_fields: list[str] | None = None,
        query: str | None = None,
        last_time: pd.Timestamp | None = None,
        time_interval: pd.Timedelta | None = None,
        columns: dict[str, str] | None = None,
        drop_constant_columns: bool = False,
        keep_columns: list[str] = [],
        sort_by_params: bool = True,
        sort_by_columns: bool = False,
        query_before_rename: bool = False,
        allow_duplicate_replacements: bool = False,
        flatten: bool = True,
    ) -> pd.DataFrame:
        """
        Queries the results file and returns a pandas DataFrame.

        Args:
            Record Selection

            - filter (dict[str, Any], optional): The MongoDB filter to apply to the query. Defaults to {}.
            - query (str, optional): A pandas `query` string to select rows from the resulting DataFrame. Defaults to None.
            - last_time (pd.Timestamp | None, optional): Filter results older than this time. Defaults to None.
            - time_interval (pd.Timedelta | None, optional): Filter results older than now - time_interval. Defaults to None.
            - query_before_rename (bool, optional): If True, applies the `query` before renaming columns. Defaults to False.

            Column Selection and Renaming

            - columns (dict[str,str] | None, optional): A dictionary where keys are columns to keep and values are their new names.
                If None, all columns are kept. Defaults to None.
            - exclude_fields (list[str] | None, optional): A list of fields to exclude from the results at the database level.
                Defaults to None.
            - drop_constant_columns (bool, optional): If True, drops columns where all values are the same.
                `None` values are treated as equal, as are `np.nan` values. Lists and dicts are
                compared by their content. Defaults to False.
            - keep_columns (list[str], optional): List of columns to keep, even if they are constant.
                Used in conjunction with `drop_constant_columns=True`. Defaults to [].
            - allow_duplicate_replacements (bool, optional): If True, allows duplicate names in the values of the
                `columns` dictionary. Defaults to False.

            Record Sorting

            - sort_by_params (bool, optional): If True, sorts the DataFrame by parameter fields and then by date.
                Defaults to True.
            - sort_by_columns (bool, optional): If True, sorts the DataFrame by the columns specified in the `columns`
                argument. Defaults to False.

            Data Formatting

                - flatten (bool, optional): Whether to flatten nested dictionaries before converting
                  to DataFrame. Defaults to False.

        Returns:
            pd.DataFrame: A DataFrame containing the filtered results.
        """
        # get defaults
        if columns is None:
            columns = self.columns

        projection = None
        if exclude_fields:
            projection = {field: 0 for field in exclude_fields}

        data: list[dict[str, Any]] = list(self.collection.find(filter, projection))
        # print(f"TrackResultsMongoDB.get: collection.find() found {len(data)} records")

        if data:
            if flatten:
                df = pd.json_normalize(data, sep=FLATTEN_SEPARATOR)
                if "_id" in df.columns:
                    df = df.set_index("_id")
            else:
                df = pd.DataFrame.from_records(data, index="_id")
        else:
            return pd.DataFrame()

        # sort by parameters
        if sort_by_params:
            parameter_fields = df.columns[
                df.columns.str.startswith("parameters" + FLATTEN_SEPARATOR)
            ].to_list()
            parameter_fields.append("date")
            # print(f"sorting by {parameter_fields}")
            df.sort_values(
                by=parameter_fields,
                # The key applies to each element in the series, not the series itself.
                # This handles unhashable types like dicts by converting them to strings for sorting.
                key=lambda x: x.map(str) if x.dtype == "object" else x,
                inplace=True,
            )

        # filter by time
        for row in df.iterrows():
            if (last_time is not None) and (row[1]["date"] < last_time):
                df.drop(row[0], inplace=True)
            if (time_interval is not None) and (
                row[1]["date"] < pd.Timestamp.now() - time_interval
            ):
                df.drop(row[0], inplace=True)

        if query_before_rename and query is not None:
            df.query(query, inplace=True)

        # select and rename columns
        if columns is not None:
            new_names = columns.values()
            if not allow_duplicate_replacements and len(new_names) != len(
                set(new_names)
            ):
                raise ValueError(
                    f"{len(new_names)-len(set(new_names))} duplicate new names in {sorted(new_names)}"
                )
            unknown = set(columns.keys()) - set(df.columns)
            if False and len(unknown) > 0:
                raise ValueError(f"unknown columns {unknown}")
            df = df.filter(items=list(columns.keys()))
            df = df.rename(columns=columns)
            # reorder to match dictionary order
            sort_by = [c for c in columns.values() if c in df.columns]
            df = df[sort_by]

        # select by query
        if not query_before_rename and query is not None:
            df.query(query, inplace=True)

        # sort by columns
        if columns is not None and sort_by_columns:
            sort_by = [c for c in columns.values() if c in df.columns]
            # print("sort_by", sort_by)
            df.sort_values(
                by=sort_by,
                # The key applies to each element in the series, not the series itself.
                key=lambda x: x.map(str) if x.dtype == "object" else x,
                inplace=True,
            )

        # drop constant columns
        if drop_constant_columns:
            df = interesting_columns(df, keep=keep_columns)

        return df

    def remove(
        self,
        *,
        filter: dict = {},
        query: str | None = None,
        simulate: bool = True,
        silent: bool = False,
    ) -> None:
        """
        Queries the results file and returns a pandas DataFrame.

        Args:
            query (str): pandas query to select rows
        """
        data: list[dict[str, Any]] = list(self.collection.find(filter))
        # print(f"TrackResultsMongoDB.remove(filter='{filter}'): data=\n", data)

        if data:
            df = pd.DataFrame.from_records(data, index="_id")
        else:
            if not silent:
                print(
                    f"TrackResultsMongoDB.remove(filter='{filter}'): nothing to remove"
                )
            return

        if query is not None:
            to_remove = df.query(query).index
            to_remove_list = to_remove.tolist()
            # print(f"TrackResultsMongoDB.remove(query='{query}'): to_remove=\n",to_remove_list)
        else:
            to_remove = df.index
            to_remove_list = to_remove.tolist()

        if len(to_remove_list) == 0:
            if not silent:
                print("TrackResultsMongoDB.remove: nothing to remove")
            return

        if not silent:
            print(f"TrackResultsMongoDB.remove: will remove indices {to_remove}")

        if not simulate:
            self.collection.delete_many({"_id": {"$in": to_remove_list}})
        elif not silent:
            print("TrackResultsMongoDB.remove: only simulated remove")


if __name__ == "__main__":
    import unittest
    import sys

    import old.test_track_results_flatten

    # Run all tests
    suite = unittest.TestLoader().loadTestsFromModule(old.test_track_results_flatten)

    # Run the tests
    runner = unittest.TextTestRunner()
    runner.run(suite)
