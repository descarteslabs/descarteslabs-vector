import requests

from .common import API_HOST, get_token
from .property_filtering import GenericProperties


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
    response.raise_for_status()
    return response.json()


def query(product_id: str, property_filter: GenericProperties = None, aoi: dict = None):
    """Query features in a vector product.

    Parameters
    ----------
    product_id : str
        ID of the vector product.
    property_filter : GenericProperties, optional
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
    response.raise_for_status()
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
    response.raise_for_status()
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
    response.raise_for_status()
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
    response.raise_for_status()
