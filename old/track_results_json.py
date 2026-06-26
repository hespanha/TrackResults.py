"""
DEPRECATED OLDER VERSION directly saving to json file

Package to track the quality of results obtained different algorithms

Example of usage:

```python
tracker=TrackResults(filename)   # file where results will be saved (supports fsspec)

# add results to file
tracker.add(parameters,results)

# get pandas table with summary of results
df=tracker.query({class:,game:},last_number:int,last_time:time.period|time.duration,columns=())
```

"""

from __future__ import annotations
from typing import Any, TextIO

import platform
import fsspec
import json
import numpy as np
import pandas as pd

#######################
## Dictionary utilities
#######################


def equal_with_nan(a: Any, b: Any) -> bool:
    """
    Checks equality between two variables, treating NaNs as equal.

    Args:
        a (Any): First value.
        b (Any): Second value.

    Returns:
        bool: True if variables are equal or both are NaN (or arrays containing NaNs in same positions).
    """
    return (
        (a == b)  # most cases
        or (  # two float('nan')
            isinstance(a, float)
            and isinstance(b, float)
            and np.isnan(a)
            and np.isnan(b)
        )
        or (  # numpy arrays with nan
            isinstance(a, np.ndarray)
            and isinstance(b, np.ndarray)
            and np.array_equal(a, b)
        )
    )


def flatten_dict(input: dict, prefix: str = "") -> dict[str, Any]:
    """
    Flattens a nested dictionary by converting nested dictionaries using the "key1.key2. ..." notation.

    Args:
        input (dict): The dictionary to flatten.
        prefix (str, optional): The prefix to use for flattened keys. Defaults to "".

    Returns:
        dict[str, Any]: The flattened dictionary.
    """
    output = {}
    for k, v in input.items():
        if not isinstance(k, str):
            k = str(k)
        key = prefix + k
        if isinstance(v, dict):
            # print("flattening:", v)
            v = flatten_dict(v, prefix=key + ".")
            # print("flattened: ", v)
            output |= v
        elif isinstance(v, np.ndarray):
            v = np.array2string(v)
            output[key] = v
        elif isinstance(v, (int, float, str, bool, type(None))):
            output[key] = v
        else:
            output[key] = str(v)
    return output


def is_subset_dict(subset_dict: dict[str, Any], main_dict: dict[str, Any]):
    """
    Checks if subset_dict is a subset of main_dict.

    A dictionary A is a subset of dictionary B if every (key, value)
    pair in A also exists in B.
    """
    # fails for nan
    # return all(item in main_dict.items() for item in subset_dict.items())
    if False:  # DEBUG
        for key, value in subset_dict.items():
            if key in main_dict:
                print(
                    f"   subset_dict key={key}, key in main_dict={key in main_dict}, equal_with_nan(main_dict[key], value)={equal_with_nan(main_dict[key], value)}"
                )
            else:
                print(
                    f"   subset_dict key={key}, missing from main_dict (keys={list(main_dict.keys())}"
                )
        print(
            "list=",
            tuple(
                key in main_dict and equal_with_nan(main_dict[key], value)
                for key, value in subset_dict.items()
            ),
        )
        print(
            "return=",
            all(
                key in main_dict and equal_with_nan(main_dict[key], value)
                for key, value in subset_dict.items()
            ),
        )
    return all(
        key in main_dict and equal_with_nan(main_dict[key], value)
        for key, value in subset_dict.items()
    )


#########################
## Class to track results
#########################


class TrackResultsJSON:
    """
    Class to track and save results of experiments to a JSON file.
    """

    __slots__ = ["filename", "query", "last_time", "time_interval", "columns"]

    filename: str
    query: str | None
    last_time: pd.Timestamp | None
    time_interval: pd.Timedelta | None
    columns: dict[str, str] | None

    def __init__(self, filename: str) -> None:
        """
        Initializes the TrackResults instance.

        Args:
            filename (str): The path to the file where results will be saved.
        """
        self.filename: str = filename
        self.query = None
        self.last_time = None
        self.time_interval = None
        self.columns = None

    def read(
        self, lock: bool = False, empty_default: bool = False
    ) -> list[dict[str, Any]]:
        """
        Reads data from the results file.

        Args:
            lock (bool, optional): Whether to lock the file during read. Defaults to False.
            empty_default (bool, optional): If True, returns an empty list if the file does not exist.
                Otherwise, raises FileNotFoundError. Defaults to False.

        Returns:
            list[dict[str, Any]]: The list of records read from the file.
        """
        assert not lock, "locking not implemented"
        try:
            with fsspec.open(self.filename, "rt") as f:
                data = json.load(f)  # type: ignore # FIXME
        except FileNotFoundError:
            print(f"track_results: file not found {self.filename}")
            if empty_default:
                data = []
            else:
                raise
        return data

    def write(self, data: list[dict[str, Any]], unlock: bool = False) -> None:
        """
        Writes data to the results file.

        Args:
            data (list[dict[str, Any]]): The list of records to write.
            unlock (bool, optional): Whether to unlock the file after write. Defaults to False.
        """
        assert not unlock, "locking not implemented"
        data_str = json.dumps(data, indent=4)
        with fsspec.open(self.filename, "wt") as f:
            f.write(data_str)  # type: ignore # FIXME
        # assert (
        #     len(self.__dict__) == 0
        # ), f"`__slots__` incomplete for `{self.__class__.__name__}`, add {list(self.__dict__.keys())}"

    def add(
        self, parameters: dict[str, Any], results: dict[str, Any], replace: bool = False
    ) -> None:
        """
        Adds a new result record to the tracking file.

        Args:
            parameters (dict[str, Any]): The parameters used for the experiment.
            results (dict[str, Any]): The results obtained from the experiment.
        """
        ## not safe
        data = self.read(empty_default=True, lock=False)
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
                "date": str(pd.Timestamp.now()),
                "platform": platform_dict,
                "parameters": parameters,
                "results": results,
            }
        )

        if replace:
            for i, rec in enumerate(data):
                if False:  # DEBUG
                    print("checking record", i)
                    print("   fields2match keys=", list(fields2match.keys()))
                    print("   rec          keys=", list(rec.keys()))
                if is_subset_dict(fields2match, rec):
                    if False:  # DEBUG
                        print("remove record=", rec)
                    data.remove(rec)
                    break

        # print("add record=", record)

        data.append(record)
        self.write(data, unlock=False)

    def __str__(self):
        data = self.read(empty_default=True, lock=False)
        return str(data)

    def default_query(
        self,
        query: str | None = None,
        last_time: pd.Timestamp | None = None,
        time_interval: pd.Timedelta | None = None,
        columns: dict[str, str] | None = None,
    ) -> None:
        if query is not None:
            self.query = query
            print(f"setting default query to '{query}'")
        if self.last_time is not None:
            self.last_time = last_time
            print(f"setting default last_time to '{last_time}'")
        if time_interval is not None:
            self.time_interval = time_interval
            print(f"setting default time_interval to '{time_interval}'")
        if columns is not None:
            self.columns = columns
            print(f"setting default columns to '{columns}'")

    def get(
        self,
        *,
        filter: dict = {},
        query: str | None = None,
        last_time: pd.Timestamp | None = None,
        time_interval: pd.Timedelta | None = None,
        columns: dict[str, str] | None = None,
        drop_constant_columns: bool = False,
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
        assert filter == {}, "filter not implemented for TrackResults (json version)"

        # get defaults
        if query is None:
            query = self.query
        if columns is None:
            columns = self.columns
        if last_time is None:
            last_time = self.last_time
        if time_interval is None:
            time_interval = self.time_interval

        data: list[dict[str, Any]] = self.read(empty_default=True, lock=False)
        if data:
            df = pd.DataFrame.from_records(data)
        else:
            return pd.DataFrame()

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])

        # sort by parameters
        if sort_by_params:
            parameter_fields = df.columns[
                df.columns.str.startswith("parameters.")
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
        if drop_constant_columns and len(df) > 1:
            cols_to_drop = []
            for col_name, col_data in df.items():
                # print(
                #     f"checking if '{col_name}' is constant data type={col_data.dtype} numpy type={col_numpy.dtype}"
                # )
                if True:
                    if col_data.nunique(dropna=False) <= 1:
                        cols_to_drop.append(col_name)
                else:
                    # FIXME: this can probably be fixed by simply using `col_data.nunique(dropna=False)`
                    # must check NaN because these will disappear with .nunique
                    col_numpy = col_data.to_numpy()
                    # print(
                    #     f"checking if '{col_name}' is constant data type={col_data.dtype} numpy type={col_numpy.dtype}"
                    # )
                    any_nan = True
                    any_not_nan = False
                    for item in col_numpy:
                        # FIXME there must be a better way to do this
                        try:
                            any_nan = any_nan or np.isnan(item)
                            any_not_nan = any_not_nan or not np.isnan(item)
                        except:
                            # no any
                            any_not_nan = True
                    # print(
                    #     f"checking if column '{col_name}' is constant: nunique={col_data.nunique()}, any_nan={any_nan}, any_not_nan={any_not_nan}"
                    # )
                    if (col_data.nunique() <= 1) and not (  # 0 means all nan
                        any_nan
                        and any_not_nan  # do not remove if some nan and other not
                    ):
                        cols_to_drop.append(col_name)
            if len(cols_to_drop) < len(df.columns):
                # only drop columns if not all
                for col_name in cols_to_drop:
                    df.drop(col_name, axis=1, inplace=True)
            else:
                raise ValueError("All columns dropped")

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
        assert query is not None, "query required for TrackResults in json version"
        assert len(filter) == 0, "filter not allowed for TrackResults in json version"

        ## not safe
        data = self.read(empty_default=False, lock=False)

        df = pd.DataFrame.from_records(data)
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"])

        to_remove = df.query(query).index
        to_remove_list = to_remove.tolist()

        if len(to_remove) == 0:
            print("nothing to remove")
            return

        print(f"will remove indices {to_remove}")
        # print(f"will remove list {to_remove_list}")
        # print(df.loc[to_remove, :])
        before = len(data)
        data = [d for (i, d) in enumerate(data) if i not in to_remove_list]
        print(f"table size reduced from {before} to {len(data)}")
        # print(data)

        if simulate:
            print("only simulated remove")
        else:
            self.write(data, unlock=False)


if __name__ == "__main__":
    import unittest
    import sys

    sys.path.append(".")
    import tests.test_track_results_mongodb

    # Run all tests
    suite = unittest.TestLoader().loadTestsFromModule(tests.test_track_results_mongodb)

    # Run the tests
    runner = unittest.TextTestRunner()
    runner.run(suite)
