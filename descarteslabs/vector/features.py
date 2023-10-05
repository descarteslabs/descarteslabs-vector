from __future__ import annotations

from io import BytesIO
from typing import List, Tuple

import geopandas as gpd
import requests
from descarteslabs.utils import Properties

from .common import API_HOST, get_token
from .util import backoff_wrapper, check_response


@backoff_wrapper
def add(product_id: str, dataframe: gpd.GeoDataFrame) -> gpd.GeoDataFrame:
    """Add features to a vector product.

    Parameters
    ----------
    product_id : str
        ID of the vector product.
    data : gpd.GeoDataFrame
        The GeoPandas dataframe to add.

    Returns
    -------
    gpd.GeoDataFrame
        A GeoPandas dataframe of the added features.
    """

    if not isinstance(dataframe, gpd.GeoDataFrame):
        raise TypeError(f"Unsupported data type {type(dataframe)}")

    buffer = BytesIO()
    dataframe.to_parquet(buffer, index=False)
    buffer.seek(0)

    files = {"file": ("data.parquet", buffer, "application/octet-stream")}

    response = requests.post(
        f"{API_HOST}/products/{product_id}/features",
        headers={"Authorization": get_token()},
        files=files,
    )

    check_response(response, "add feature")

    buffer = BytesIO(response.content)

    return gpd.read_parquet(buffer)


@backoff_wrapper
def query(
    product_id: str,
    property_filter: Properties = None,
    aoi: dict = None,
    columns: list = None,
) -> gpd.GeoDataFrame:
    """Query features in a vector product.

    Parameters
    ----------
    product_id : str
        ID of the vector product.
    property_filter : Properties, optional
        Property filters to filter the product with.
    aoi : dict, optional
        A GeoJSON Feature to filter the vector product with.
    columns : list, optional
        Optional list of column names.

    Returns
    -------
    gpd.GeoDataFrame
        A GeoPandas dataframe.
    """
    if property_filter is not None:
        property_filter = property_filter.serialize()
    response = requests.post(
        f"{API_HOST}/products/{product_id}/features/query",
        headers={"Authorization": get_token()},
        json={"filter": property_filter, "aoi": aoi, "columns": columns},
    )
    check_response(response, "query feature")

    buffer = BytesIO(response.content)

    return gpd.read_parquet(buffer)


@backoff_wrapper
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
) -> gpd.GeoDataFrame:
    """Join features in a vector product.

    Parameters
    ----------
    input_product_id : str
        Product ID of the input table.
    join_product_id : str
        Product ID of the join table.
    join_type : str
        String indicating the type of join to perform.
        Must be one of INNER, LEFT, or RIGHT.
    join_columns : List[Tuple[str, str]]
        List of columns to join the input and join table.
        [(input_table.col1, join_table.col2), ...]
    include_columns : List[Tuple[str, ...]]
        List of columns to include from either side of
        the join formatter as [(input_table.col1, input_table.col2),
        (join_table.col3, join_table.col4)]. If None, all columns
        from both tables are returned.
    input_property_filter : Properties
        Property filters to filter the input table.
    join_property_filter : Properties
        Property filters to filter the join table.
    input_aoi : dict
        A GeoJSON Feature to filter the input table.
    join_aoi : dict
        A GeoJSON Feature to filter the join table.
    Returns
    -------
    gpd.GeoDataFrame
        A GeoPandas dataframe.
    """
    if input_property_filter is not None:
        input_property_filter = input_property_filter.serialize()

    if join_property_filter is not None:
        join_property_filter = join_property_filter.serialize()

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
    }

    response = requests.post(
        f"{API_HOST}/products/features/join",
        headers={"Authorization": get_token()},
        json=params,
    )
    check_response(response, "join feature")

    buffer = BytesIO(response.content)

    return gpd.read_parquet(buffer)


@backoff_wrapper
def get(product_id: str, feature_id: str) -> gpd.GeoDataFrame:
    """Get a feature from a vector product.

    Parameters
    ----------
    product_id : str
        ID of the vector product.
    feature_id : str
        ID of the feature.

    Returns
    -------
    gpd.GeoDataFrame
        A GeoPandas dataframe.
    """
    response = requests.get(
        f"{API_HOST}/products/{product_id}/features/{feature_id}",
        headers={"Authorization": get_token()},
    )

    check_response(response, "get feature")

    buffer = BytesIO(response.content)

    return gpd.read_parquet(buffer)


@backoff_wrapper
def update(product_id: str, feature_id: str, dataframe: gpd.GeoDataFrame) -> None:
    """Update a feature in a vector product.

    Parameters
    ----------
    product_id : str
        ID of the vector product.
    feature_id : str
        ID of the feature.
    dataframe : gpd.GeoDataFrame
        A GeoPandas dataframe to replace the feature with.

    Returns
    -------
    None
    """

    if not isinstance(dataframe, gpd.GeoDataFrame):
        raise TypeError(f"Unsupported data type {type(dataframe)}")

    if dataframe.shape[0] != 1:
        raise ValueError("Only 1 row can be updated!")

    buffer = BytesIO()
    dataframe.to_parquet(buffer, index=False)
    buffer.seek(0)

    files = {"file": ("data.parquet", buffer, "application/octet-stream")}

    response = requests.put(
        f"{API_HOST}/products/{product_id}/features/{feature_id}",
        headers={"Authorization": get_token()},
        files=files,
    )

    check_response(response, "update feature")


@backoff_wrapper
def delete(product_id: str, feature_id: str):
    """Delete a feature in a vector product.

    Parameters
    ----------
    product_id : str
        ID of the vector product.
    feature_id : str
        ID of the feature.
    """

    response = requests.delete(
        f"{API_HOST}/products/{product_id}/features/{feature_id}",
        headers={"Authorization": get_token()},
    )

    check_response(response, "delete feature")
