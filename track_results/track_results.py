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
from typing import Any

import pydantic

import platform
import uuid
import numpy as np
import pandas as pd
import datetime
import pprint

#######################
## Dictionary utilities
#######################

FLATTEN_SEPARATOR = "_"

TYPES_TO_KEEP_AS_IS = (
    type(None),
    bool,
    int,
    float,
    bytes,
    str,
    pd.Timestamp,
    datetime.datetime,
    list,
    dict,
    uuid.UUID,
)

# not natively supported by pymongo, but supported when converted to list
TYPE_CONVERT_TO_LIST = (
    tuple,
    set,
)


def flatten_dict(input: dict, prefix: str = "") -> dict[str, Any]:
    """
    Flattens a nested dictionary by converting nested dictionaries using the "key1_key2_ ..." notation.

    Args:
        input (dict): The dictionary to flatten.
        prefix (str, optional): The prefix to use for flattened keys. Defaults to "".
        prefix (str, optional): The separator used to separate keys. Defaults to "_".
                                ("." is problematic for mongodb)

    Returns:
        dict[str, Any]: The flattened dictionary.
    """
    output = {}
    for k, v in input.items():
        k = k.replace(".", FLATTEN_SEPARATOR)
        if not isinstance(k, str):
            k = str(k)
        key = prefix + k
        if isinstance(v, dict):
            # print("flattening:", v)
            v = flatten_dict(v, prefix=key + FLATTEN_SEPARATOR)
            # print("flattened: ", v)
            output |= v
        # FIXME conversion to string makes sure table is sortable, but makes searches and sorting
        # more difficult.
        # FIXME might not be needed with pymongoarrow (but needed with mongita)
        elif isinstance(v, np.ndarray):
            # v = np.array2string(v)
            v = (
                v.tolist()
            )  # supported by pymongo and mongita (unlike np.ndarray, and keeps the array structure
            output[key] = v
        elif isinstance(v, TYPE_CONVERT_TO_LIST):
            output[key] = list(v)
        elif isinstance(v, TYPES_TO_KEEP_AS_IS):
            output[key] = v
        else:  # if all else fails...
            output[key] = str(v)
    return output


_hashable = (
    "str",
    "int",
    "float",
    "bool",
    "bytes",
    tuple[int, ...],
    tuple[float, ...],
)


def interesting_column(df, col) -> bool:
    if df[col].dtype in _hashable:
        # gets messed up by some nan
        n_unique = df[col].nunique(dropna=False)
        interesting = n_unique > 1
    else:
        n_unique = None
        try:
            # solves case when some columns of nan get get mapped to dtype object
            vals = set(df[col])
            n_vals = len(vals)
            interesting = n_vals > 1
        except:
            vals = None
            n_vals = None
            interesting = True

    return interesting


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

    def drop(self, simulate: bool = True):
        if simulate:
            count = self.collection.count_documents({})
            print(
                f"TrackResultsMongoDB.drop: simulated drop of collection={self.collection.name} with {count} documents"
            )
        else:
            count = self.collection.count_documents({})
            self.database.drop_collection(self.collection.name)
            print(
                f"TrackResultsMongoDB.drop: drop of collection={self.collection.name} with {count} documents"
            )

    def add(
        self,
        parameters: dict[str, Any] | pydantic.BaseModel,
        results: dict[str, Any] | pydantic.BaseModel,
        replace: bool = False,
        flatten: bool = True,
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

        fields2match = {"parameters": parameters, "platform": platform_dict}
        full_record = {
            "date": pd.Timestamp.now(),
            "platform": platform_dict,
            "parameters": parameters,
            "results": results,
        }

        if flatten:
            fields2match = flatten_dict(fields2match)
            full_record = flatten_dict(full_record)

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
            print("rc:", rc)
            print(
                f"TrackResultsMongoDB.add(replace=False): inserted record with id={rc.inserted_id}"
            )

    def __str__(self):
        data = list(self.collection.find({}))
        return str(data)

    def get(
        self,
        *,
        filter: dict[str, Any] = {},
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
            query (str): pandas query to select rows
            last_time (pd.Timestamp | None, optional): Filter results older than this time. Defaults to None.
            time_interval (pd.Timedelta | None, optional): Filter results older than now - time_interval. Defaults to None.
            columns (dict[str,str] | None, optional): Dictionary of with columns to keep (keys)
                and new names (values). Defaults to None.
            flatten (bool, optional): Whether to flatten nested dictionaries before converting
                to DataFrame. Defaults to True.


        Returns:
            pd.DataFrame: A DataFrame containing the filtered results.
        """
        # get defaults
        if columns is None:
            columns = self.columns

        data: list[dict[str, Any]] = list(self.collection.find(filter))
        # print(f"TrackResultsMongoDB.get: collection.find() found {len(data)} records")

        if flatten:
            data = [flatten_dict(record) for record in data]

        if data:
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
                key=lambda col: (
                    col.astype(str) if col.dtype == "object" else col
                ),  # protection against unhashable types (e.g. dicts) that might be in parameters, but still sort them in a deterministic way
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
                key=lambda col: (
                    col.astype(str) if col.dtype == "object" else col
                ),  # protection against unhashable types (e.g. dicts) that might be in parameters, but still sort them in a deterministic way
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
            print(f"TrackResultsMongoDB.remove(filter='{filter}'): nothing to remove")
            return

        if query is not None:
            to_remove = df.query(query).index
            to_remove_list = to_remove.tolist()
            # print(f"TrackResultsMongoDB.remove(query='{query}'): to_remove=\n",to_remove_list)
        else:
            to_remove = df.index
            to_remove_list = to_remove.tolist()

        if len(to_remove_list) == 0:
            print("TrackResultsMongoDB.remove: nothing to remove")
            return

        print(f"TrackResultsMongoDB.remove: will remove indices {to_remove}")

        if simulate:
            print("TrackResultsMongoDB.remove: only simulated remove")
        else:
            self.collection.delete_many({"_id": {"$in": to_remove_list}})


if __name__ == "__main__":
    import unittest
    import sys

    import tests.test_track_results_flatten

    # Run all tests
    suite = unittest.TestLoader().loadTestsFromModule(tests.test_track_results_flatten)

    # Run the tests
    runner = unittest.TextTestRunner()
    runner.run(suite)
