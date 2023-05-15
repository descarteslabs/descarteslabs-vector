import requests
from descarteslabs.utils import Properties

from .common import API_HOST, get_token
from .util import check_response


def add(product_id: str, feature_collection: dict):
    """Add features (from a GeoJSON FeatureCollection) to a vector product.

    Parameters
    ----------
    product_id : str
        ID of the vector product.
    feature_collection : dict
        The GeoJSON FeatureCollection to add.

    Returns
    -------
    dict
        A GeoJSON FeatureCollection of the added features.
    """
    response = requests.post(
        f"{API_HOST}/products/{product_id}/features",
        headers={"Authorization": get_token()},
        json={"feature_collection": feature_collection},
    )
    check_response(response, "add feature")
    return response.json()


def query(product_id: str, property_filter: Properties = None, aoi: dict = None):
    """Query features in a vector product.

    Parameters
    ----------
    product_id : str
        ID of the vector product.
    property_filter : Properties, optional
        Property filters to filter the product with.
    aoi : dict, optional
        A GeoJSON Feature to filter the vector product with.

    Returns
    -------
    dict
        A GeoJSON FeatureCollection of the queried features.
    """
    if property_filter is not None:
        property_filter = property_filter.serialize()
    response = requests.post(
        f"{API_HOST}/products/{product_id}/features/query",
        headers={"Authorization": get_token()},
        json={"filter": property_filter, "aoi": aoi},
    )
    check_response(response, "query feature")
    return response.json()


def get(product_id: str, feature_id: str):
    """Get a feature from a vector product.

    Parameters
    ----------
    product_id : str
        ID of the vector product.
    feature_id : str
        ID of the feature.

    Returns
    -------
    dict
        A GeoJSON Feature.
    """
    response = requests.get(
        f"{API_HOST}/products/{product_id}/features/{feature_id}",
        headers={"Authorization": get_token()},
    )
    check_response(response, "get feature")
    return response.json()


def update(product_id: str, feature_id: str, feature: dict):
    """Update a feature in a vector product.

    Parameters
    ----------
    product_id : str
        ID of the vector product.
    feature_id : str
        ID of the feature.
    feature : dict
        The GeoJSON Feature to replace the feature with.

    Returns
    -------
    dict
        A GeoJSON feature.
    """
    response = requests.put(
        f"{API_HOST}/products/{product_id}/features/{feature_id}",
        headers={"Authorization": get_token()},
        json={"feature": feature},
    )
    check_response(response, "update feature")
    return response.json()


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
