from __future__ import annotations

from copy import deepcopy
from enum import Enum
from io import BytesIO
from typing import List, Tuple, Union

import geopandas as gpd
import pandas as pd
import requests
from descarteslabs.utils import Properties

from .common import API_HOST, TYPES, get_token
from .util import backoff_wrapper, check_response, response_to_dataframe

REQUEST_TIMEOUT = 60


class Statistic(str, Enum):
    """
    A class for aggregate statistics.
    """

    COUNT = "COUNT"
    SUM = "SUM"
    MIN = "MIN"
    MAX = "MAX"
    MEAN = "MEAN"


@backoff_wrapper
def add(
    product_id: str, dataframe: Union[gpd.GeoDataFrame, pd.DataFrame], is_spatial: bool
) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Add features to a Vector Table.

    Parameters
    ----------
    product_id : str
        Product ID of the Vector Table.
    dataframe : Union[gpd.GeoDataFrame, pd.DataFrame]
        A GeoPandas GeoDataFrame or a Pandas DataFrame to add.
    is_spatial: bool
        Boolean indicating whether or not this data is spatial.
    Returns
    -------
    Union[gpd.GeoDataFrame, pd.DataFrame]
    """

    if not is_spatial and not isinstance(dataframe, pd.DataFrame):
        raise TypeError("'dataframe' must be of type <pd.DataFrame>!")
    elif is_spatial and not isinstance(dataframe, gpd.GeoDataFrame):
        raise TypeError("'dataframe' must be of type <gpd.GeoDataFrame>!")

    buffer = BytesIO()
    dataframe.to_parquet(buffer, index=False)
    buffer.seek(0)

    files = {"file": ("vector.parquet", buffer, "application/octet-stream")}

    response = requests.post(
        f"{API_HOST}/products/{product_id}/features",
        headers={"Authorization": get_token(), "is_spatial": str(is_spatial)},
        files=files,
        timeout=REQUEST_TIMEOUT,
    )

    check_response(response, "add feature")

    return response_to_dataframe(response=response)


@backoff_wrapper
def query(
    product_id: str,
    property_filter: Properties = None,
    aoi: dict = None,
    columns: list = None,
) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Query features in a Vector Table.

    Parameters
    ----------
    product_id : str
        Product ID of the Vector Table.
    property_filter : Properties, optional
        Property filters to filter the product with.
    aoi : dict, optional
        A GeoJSON Feature to filter the vector product with.
    columns : list, optional
        Optional list of column names.

    Returns
    -------
    Union[gpd.GeoDataFrame, pd.DataFrame]
    """
    if property_filter is not None:
        property_filter = property_filter.serialize()
    response = requests.post(
        f"{API_HOST}/products/{product_id}/features/query",
        headers={"Authorization": get_token()},
        json={"filter": property_filter, "aoi": aoi, "columns": columns},
        timeout=REQUEST_TIMEOUT,
    )
    check_response(response, "query feature")

    return response_to_dataframe(response=response)


@backoff_wrapper
def _join(params: dict) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Internal join function.

    Parameters
    ----------
    params : dict
        Dictionary of parameters to pass to the join endpoint.

    Returns
    -------
    Union[gpd.GeoDataFrame, pd.DataFrame]
    """
    params = deepcopy(params)

    input_property_filter = params.get("input_property_filter", None)
    if input_property_filter is not None:
        params["input_property_filter"] = input_property_filter.serialize()

    join_property_filter = params.get("join_property_filter", None)
    if join_property_filter is not None:
        params["join_property_filter"] = join_property_filter.serialize()

    response = requests.post(
        f"{API_HOST}/products/features/join",
        headers={"Authorization": get_token()},
        json=params,
        timeout=REQUEST_TIMEOUT,
    )
    check_response(response, "join feature")

    return response_to_dataframe(response=response)


def join(
    input_product_id: str,
    join_product_id: str,
    join_type: str,
    join_columns: List[Tuple[str, str]],
    include_columns: List[Tuple[str, ...]] = None,
    input_property_filter: Properties = None,
    join_property_filter: Properties = None,
    input_aoi: dict = None,
    join_aoi: dict = None,
) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Execute relational join between two Vector Tables.

    Parameters
    ----------
    input_product_id : str
        Product ID of the input Vector Table.
    join_product_id : str
        Product ID of the join Vector Table.
    join_type : str
        String indicating the type of join to perform.
        Must be one of INNER, LEFT, RIGHT, INTERSECTS,
            CONTAINS, OVERLAPS, WITHIN.
    join_columns : List[Tuple[str, str]]
        List of columns to join the input and join Vector Table.
        [(input_table.col1, join_table.col2), ...]
    include_columns : List[Tuple[str, ...]]
        List of columns to include from either side of
        the join formatted as [(input_table.col1, input_table.col2),
        (join_table.col3, join_table.col4)]. If None, all columns
        from both Vector Tables are returned.
    input_property_filter : Properties
        Property filters to filter the input Vector Table.
    join_property_filter : Properties
        Property filters to filter the join Vector Table.
    input_aoi : dict
        A GeoJSON Feature to filter the input Vector Table.
    join_aoi : dict
        A GeoJSON Feature to filter the join Vector Table.
    Returns
    -------
    Union[gpd.GeoDataFrame, pd.DataFrame]
    """

    params = {
        "input_product_id": input_product_id,
        "join_type": join_type,
        "join_product_id": join_product_id,
        "join_columns": join_columns,
        "include_columns": include_columns,
        "input_property_filter": input_property_filter,
        "input_aoi": input_aoi,
        "join_property_filter": join_property_filter,
        "join_aoi": join_aoi,
        "keep_all_input_rows": False,  # not used with non-spatial join
    }

    return _join(params)


def sjoin(
    input_product_id: str,
    join_product_id: str,
    join_type: str,
    include_columns: List[Tuple[str, ...]] = None,
    input_property_filter: Properties = None,
    join_property_filter: Properties = None,
    input_aoi: dict = None,
    join_aoi: dict = None,
    keep_all_input_rows: bool = False,
) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Execute spatial join between two Vector Tables.

    Parameters
    ----------
    input_product_id : str
        Product ID of the input Vector Table.
    join_product_id : str
        Product ID of the join Vector Table.
    join_type : str
        String indicating the type of join to perform.
        Must be one of INNER, LEFT, RIGHT, INTERSECTS,
            CONTAINS, OVERLAPS, WITHIN.
    include_columns : List[Tuple[str, ...]]
        List of columns to include from either side of
        the join formatted as [(input_table.col1, input_table.col2),
        (join_table.col3, join_table.col4)]. If None, all columns
        from both Vector Tables are returned.
    input_property_filter : Properties
        Property filters to filter the input Vector Table.
    join_property_filter : Properties
        Property filters to filter the join Vector Table.
    input_aoi : dict
        A GeoJSON Feature to filter the input Vector Table.
    join_aoi : dict
        A GeoJSON Feature to filter the join Vector Table.
    keep_all_input_rows : bool
        Boolean indicating if the spatial join should keep all input rows
        whether they satisfy the spatial query or not.
    Returns
    -------
    Union[gpd.GeoDataFrame, pd.DataFrame]
    """

    params = {
        "input_product_id": input_product_id,
        "join_type": join_type,
        "join_product_id": join_product_id,
        "join_columns": None,  # not used with spatial join
        "include_columns": include_columns,
        "input_property_filter": input_property_filter,
        "input_aoi": input_aoi,
        "join_property_filter": join_property_filter,
        "join_aoi": join_aoi,
        "keep_all_input_rows": keep_all_input_rows,
    }

    return _join(params)


@backoff_wrapper
def get(product_id: str, feature_id: str) -> Union[gpd.GeoDataFrame, pd.DataFrame]:
    """
    Get a feature from a Vector Table.

    Parameters
    ----------
    product_id : str
        Product ID of the Vector Table.
    feature_id : str
        ID of the feature.

    Returns
    -------
    Union[gpd.GeoDataFrame, pd.DataFrame]
        A Pandas or GeoPandas dataframe.
    """
    response = requests.get(
        f"{API_HOST}/products/{product_id}/features/{feature_id}",
        headers={"Authorization": get_token()},
        timeout=REQUEST_TIMEOUT,
    )

    check_response(response, "get feature")

    return response_to_dataframe(response=response)


@backoff_wrapper
def update(
    product_id: str,
    feature_id: str,
    dataframe: Union[gpd.GeoDataFrame, pd.DataFrame],
    is_spatial: bool,
) -> None:
    """
    Save/update a feature in a Vector Table.

    Parameters
    ----------
    product_id : str
        Product ID of the Vector Table.
    feature_id : str
        ID of the feature.
    dataframe : Union[gpd.GeoDataFrame, pd.DataFrame]
        A GeoPandas GeoDataFrame or a Pandas DataFrame to replace
        the feature with.
    is_spatial : bool
        Boolean indicating whether or not this data is spatial.
    Returns
    -------
    None
    """
    if not isinstance(dataframe, TYPES):
        raise TypeError(f"Unsupported data type {type(dataframe)}")

    if dataframe.shape[0] != 1:
        raise ValueError("Only 1 row can be updated!")

    buffer = BytesIO()
    dataframe.to_parquet(buffer, index=False)
    buffer.seek(0)

    files = {"file": ("vector.parquet", buffer, "application/octet-stream")}

    response = requests.put(
        f"{API_HOST}/products/{product_id}/features/{feature_id}",
        headers={"Authorization": get_token(), "is_spatial": str(is_spatial)},
        files=files,
        timeout=REQUEST_TIMEOUT,
    )

    check_response(response, "update feature")


@backoff_wrapper
def aggregate(
    product_id: str,
    statistic: Statistic,
    property_filter: Properties = None,
    aoi: dict = None,
    columns: list = None,
) -> Union[int, dict]:
    """
    Calculate aggregate statistics for features in a Vector Table.
    The statistic COUNT will always return an integer. All other
    statistics will return a dictionary of results. Keys of the
    dictionary will be the column names requested appended with
    the statistic ('column_1.STATISTIC') and values are the result
    of the aggregate statistic.

    Parameters
    ----------
    product_id : str
        Product ID of the Vector Table
    statistic : Statistic
        Statistic to calculate.
    property_filter : Properties, optional
        Property filters to filter the product with.
    aoi : dict, optional
        A GeoJSON Feature to filter the vector product with.
    columns : list, optional
        Optional list of column names.

    Returns
    -------
    Union[int, dict]
    """
    if not isinstance(statistic, Statistic):
        raise TypeError("'statistic' must be of type <Statistic>.")

    if property_filter is not None:
        property_filter = property_filter.serialize()
    response = requests.post(
        f"{API_HOST}/products/{product_id}/features/aggregate",
        headers={"Authorization": get_token()},
        json={
            "statistic": statistic.value,
            "filter": property_filter,
            "aoi": aoi,
            "columns": columns,
        },
        timeout=REQUEST_TIMEOUT,
    )
    check_response(response, "aggregate feature")

    return response.json()


@backoff_wrapper
def delete(product_id: str, feature_id: str):
    """
    Delete a feature in a Vector Table.

    Parameters
    ----------
    product_id : str
        Product ID of the Vector Table.
    feature_id : str
        ID of the feature.
    """

    response = requests.delete(
        f"{API_HOST}/products/{product_id}/features/{feature_id}",
        headers={"Authorization": get_token()},
        timeout=REQUEST_TIMEOUT,
    )

    check_response(response, "delete feature")
