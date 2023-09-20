import io

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

    buffer = io.BytesIO()
    dataframe.to_parquet(buffer, index=False)
    buffer.seek(0)

    files = {"file": ("data.parquet", buffer, "application/octet-stream")}

    response = requests.post(
        f"{API_HOST}/products/{product_id}/features",
        headers={"Authorization": get_token()},
        files=files,
    )

    check_response(response, "add feature")

    buffer = io.BytesIO(response.content)

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

    buffer = io.BytesIO(response.content)

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

    buffer = io.BytesIO(response.content)

    return gpd.read_parquet(buffer)


@backoff_wrapper
def update(
    product_id: str, feature_id: str, dataframe: gpd.GeoDataFrame
) -> gpd.GeoDataFrame:
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
    gpd.GeoDataFrame
        A GeoPandas dataframe of the modified feature.
    """

    if not isinstance(dataframe, gpd.GeoDataFrame):
        raise TypeError(f"Unsupported data type {type(dataframe)}")

    if dataframe.shape[0] != 1:
        raise ValueError("Only 1 row can be updated!")

    buffer = io.BytesIO()
    dataframe.to_parquet(buffer, index=False)
    buffer.seek(0)

    files = {"file": ("data.parquet", buffer, "application/octet-stream")}

    response = requests.put(
        f"{API_HOST}/products/{product_id}/features/{feature_id}",
        headers={"Authorization": get_token()},
        files=files,
    )

    check_response(response, "update feature")

    buffer = io.BytesIO(response.content)

    return gpd.read_parquet(buffer)


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
