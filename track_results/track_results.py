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

import platform
import numpy as np
import pandas as pd
import pprint

#######################
## Dictionary utilities
#######################

FLATTEN_SEPARATOR = "_"


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
        # FIXME conversion to string makes sure table is sortable
        # FIXME might not be needed with pymongoarrow (but needed with mongita)
        elif isinstance(v, np.ndarray):
            v = np.array2string(v)
            output[key] = v
        elif not isinstance(v, (int, float, str, bool, type(None), pd.Timestamp)):
            output[key] = str(v)
        else:
            output[key] = v
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
            vals = set(val for val in df[col])
            n_vals = len(vals)
            interesting = n_vals > 1
        except:
            vals = None
            n_vals = None
            interesting = True
    if False and interesting:
        # debug
        try:
            vals = np.unique(df[col].to_numpy())
            # n_vals = len(set(val for val in df[col]))
            n_vals = len(vals)
        except:
            vals = None
            n_vals = None
        print(
            f"  {col:60}:interesting={str(interesting):10} {str(df[col].dtypes):20}{df[col].dtypes.__class__.__name__:20}, hashable={df[col].dtype in _hashable:5}, n_unique={str(n_unique):5}, n_vals={n_vals}"
        )
        if n_unique is None:
            print("not hashable", df[col])
        if n_vals is not None and n_unique is not None and n_vals != n_unique:
            print("mismatch", vals)
            if interesting:
                raise ValueError(
                    'n_unique and n_vals mismatch for columns that was deemed "interesting", probably need fixing'
                )

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
            if isinstance(df.iloc[0][col], float) and not np.isnan(df.iloc[0][col])
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
            print(f"TrackResultsTrackResultsMongoDB: using MongitaClientDisk")
            client = mongita.MongitaClientDisk()
        else:
            print(f"TrackResultsTrackResultsMongoDB: using MongoClient")
            client = pymongo.MongoClient(uri)
        self.database = client[database]

        if verbose:
            collection_names = self.database.list_collection_names()
            print(f"Selecting collection={collection} out of:")
            for i, name in enumerate(collection_names):
                print(f"  {i:3}: {name}")

        self.collection = self.database[collection]

        self.columns = None

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
        self, parameters: dict[str, Any], results: dict[str, Any], replace: bool = False
    ) -> None:
        """
        Adds a new result record to the tracking file.

        Args:
            parameters (dict[str, Any]): The parameters used for the experiment.
            results (dict[str, Any]): The results obtained from the experiment.
        """

        platform_dict = {
            "node": platform.node(),
            "architecture": platform.architecture(),
            "machine": platform.machine(),
            "processor": platform.processor(),
            "system": platform.system(),
            "release": platform.release(),
            "version": platform.version(),
            "python_version": platform.python_version(),
            "python_implementation": platform.python_implementation(),
        }

        fields2match = flatten_dict(
            {"parameters": parameters} | {"platform": platform_dict}
        )
        record = flatten_dict(
            {
                # FIXME: mongodb should handle dates automatically
                # "date": str(pd.Timestamp.now()),
                "date": pd.Timestamp.now(),
                "platform": platform_dict,
                "parameters": parameters,
                "results": results,
            }
        )
        # pprint.pprint(record, indent=3)

        if replace:
            filter = fields2match
            rc = self.collection.replace_one(
                filter, record, upsert=True  # add if not there
            )
            # print("filter:", filter)
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
            rc = self.collection.insert_one(record)
            print("rc:", rc)
            print(
                f"TrackResultsMongoDB.add(replace=False): inserted record with id={rc.inserted_id}"
            )

    def __str__(self):
        data = [doc for doc in self.collection.find({})]
        return str(data)

    def get(
        self,
        *,
        filter: dict = {},
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
    ) -> pd.DataFrame:
        """
        Queries the results file and returns a pandas DataFrame.

        Args:
            query (str): pandas query to select rows
            last_time (pd.Timestamp | None, optional): Filter results older than this time. Defaults to None.
            time_interval (pd.Timedelta | None, optional): Filter results older than now - time_interval. Defaults to None.
            columns (dict[str,str] | None, optional): Dictionary of with columns to keep (keys) and new names (values). Defaults to None.


        Returns:
            pd.DataFrame: A DataFrame containing the filtered results.
        """
        # get defaults
        if columns is None:
            columns = self.columns

        data: list[dict[str, Any]] = [doc for doc in self.collection.find(filter)]
        # print(f"TrackResultsMongoDB.get: collection.find() found {len(data)} records")

        if data:
            df = pd.DataFrame.from_records(data, index="_id")
        else:
            return pd.DataFrame()

        # FIXME: mongodb should handle dates automatically
        # if "date" in df.columns:
        #    df["date"] = pd.to_datetime(df["date"])

        # sort by parameters
        if sort_by_params:
            parameter_fields = df.columns[
                df.columns.str.startswith("parameters" + FLATTEN_SEPARATOR)
            ].to_list()
            parameter_fields.append("date")
            # print(f"sorting by {parameter_fields}")
            df.sort_values(by=parameter_fields, inplace=True)

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
            df.sort_values(by=sort_by, inplace=True)

        # drop constant columns
        if drop_constant_columns:
            df = interesting_columns(df, keep=keep_columns)

        # assert (
        #     len(self.__dict__) == 0
        # ), f"`__slots__` incomplete for `{self.__class__.__name__}`, add {list(self.__dict__.keys())}"
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
        data: list[dict[str, Any]] = [doc for doc in self.collection.find(filter)]
        # print(f"TrackResultsMongoDB.remove(filter='{filter}'): data=\n", data)

        if data:
            df = pd.DataFrame.from_records(data, index="_id")
        else:
            print(f"TrackResultsMongoDB.remove(filter='{filter}'): nothing to remove")
            return

        # FIXME: mongodb should handle dates automatically
        # in df.columns:
        #    df["date"] = pd.to_datetime(df["date"])

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

    sys.path.append(".")
    import tests.test_track_results

    # Run all tests
    suite = unittest.TestLoader().loadTestsFromModule(tests.test_track_results)

    # Run the tests
    runner = unittest.TextTestRunner()
    runner.run(suite)
