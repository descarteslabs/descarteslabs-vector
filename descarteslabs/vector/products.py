from typing import List, Optional, Union

import requests

from .common import API_HOST, USERAGENT, VECTOR_TIMEOUT, get_token
from .models import GenericFeatureBaseModel, VectorBaseModel
from .util import backoff_wrapper, check_response
from .vector_exceptions import ClientException


def _check_tags(tags: Union[List[str], None] = None):
    if tags:
        for tag in tags:
            if tag.find(",") >= 0:
                raise ClientException('tags cannot contain ","')


def _strip_null_values(d: dict) -> dict:
    """Strip null (ie. None) values from a dictionary.

    This is used to strip null values from request query strings.

    Parameters
    ----------
    d : dict
        The input dictionary.

    Returns
    -------
    dict
    """
    return {k: v for k, v in d.items() if v is not None}


@backoff_wrapper
def create(
    product_id: str,
    name: str,
    description: Optional[str] = None,
    tags: Optional[List[str]] = None,
    readers: Optional[List[str]] = None,
    writers: Optional[List[str]] = None,
    owners: Optional[List[str]] = None,
    model: Optional[VectorBaseModel] = GenericFeatureBaseModel,
) -> dict:
    """
    Create a Vector Table.

    Parameters
    ----------
    product_id : str
        Product ID of a Vector Table.
    name : str
        Name of the Vector Table.
    description : str, optional
        Description of the Vector Table.
    tags : list of str, optional
        A list of tags to associate with the Vector Table.
    readers : list of str, optional
        A list of Vector Table readers. Can take the form "user:{namespace}", "group:{group}", "org:{org}", or
        "email:{email}".
    writers : list of str, optional
        A list of Vector Table writers. Can take the form "user:{namespace}", "group:{group}", "org:{org}", or
        "email:{email}".
    owners : list of str, optional
        A list of Vector Table owners. Can take the form "user:{namespace}", "group:{group}", "org:{org}", or
        "email:{email}".
    model : VectorBaseModel, optional
        A json schema describing the table

    Returns
    -------
    dict
    """
    _check_tags(tags)

    if "geometry" in model.model_json_schema()["properties"].keys():
        is_spatial = True
    else:
        is_spatial = False

    request_json = _strip_null_values(
        {
            "id": product_id,
            "name": name,
            "is_spatial": is_spatial,
            "description": description,
            "tags": tags,
            "readers": readers,
            "writers": writers,
            "owners": owners,
            "model": model.model_json_schema(),
        }
    )

    response = requests.post(
        f"{API_HOST}/products/",
        headers={"Authorization": get_token(), "User-Agent": USERAGENT},
        json=request_json,
        timeout=VECTOR_TIMEOUT,
    )
    check_response(response, "create product")
    return response.json()


@backoff_wrapper
def list(tags: Union[List[str], None] = None) -> List[dict]:
    """
    List Vector Tables.

    Parameters
    ----------
    tags: List[str]
        Optional list of tags a Vector Table must have to be included in the returned list.

    Returns
    -------
    List[dict]
    """
    _check_tags(tags)

    if tags:
        response = requests.get(
            f"{API_HOST}/products/",
            headers={"Authorization": get_token(), "User-Agent": USERAGENT},
            params={"tags": ",".join(tags)},
            timeout=VECTOR_TIMEOUT,
        )
    else:
        response = requests.get(
            f"{API_HOST}/products/",
            headers={"Authorization": get_token(), "User-Agent": USERAGENT},
            timeout=VECTOR_TIMEOUT,
        )
    check_response(response, "list products")
    return response.json()


@backoff_wrapper
def get(product_id: str) -> dict:
    """
    Get a Vector Table.

    Parameters
    ----------
    product_id : str
        Product ID of a Vector Table.

    Returns
    -------
    dict
    """
    response = requests.get(
        f"{API_HOST}/products/{product_id}",
        headers={"Authorization": get_token(), "User-Agent": USERAGENT},
        timeout=VECTOR_TIMEOUT,
    )
    check_response(response, "get product")
    return response.json()


@backoff_wrapper
def update(
    product_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    tags: Optional[List[str]] = None,
    readers: Optional[List[str]] = None,
    writers: Optional[List[str]] = None,
    owners: Optional[List[str]] = None,
) -> dict:
    """
    Save/update a Vector Table.

    Parameters
    ----------
    product_id : str
        Product ID of a Vector Table.
    name : str
        Name of the Vector Table.
    description : str, optional
        Description of the Vector Table.
    tags : list of str, optional
        A list of tags to associate with the Vector Table.
    readers : list of str, optional
        A list of Vector Table readers. Can take the form "user:{namespace}", "group:{group}", "org:{org}", or
        "email:{email}".
    writers : list of str, optional
        A list of Vector Table writers. Can take the form "user:{namespace}", "group:{group}", "org:{org}", or
        "email:{email}".
    owners : list of str, optional
        A list of Vector Table owners. Can take the form "user:{namespace}", "group:{group}", "org:{org}", or
        "email:{email}".

    Returns
    -------
    dict
    """
    _check_tags(tags)
    response = requests.patch(
        f"{API_HOST}/products/{product_id}",
        headers={"Authorization": get_token(), "User-Agent": USERAGENT},
        json=_strip_null_values(
            {
                "name": name,
                "description": description,
                "tags": tags,
                "readers": readers,
                "writers": writers,
                "owners": owners,
            },
        ),
        timeout=VECTOR_TIMEOUT,
    )
    check_response(response, "update product")
    return response.json()


@backoff_wrapper
def delete(product_id: str) -> None:
    """
    Delete a Vector Table.

    Parameters
    ----------
    product_id : str
        Product ID of a Vector Table.

    Returns
    -------
    None
    """
    response = requests.delete(
        f"{API_HOST}/products/{product_id}",
        headers={"Authorization": get_token(), "User-Agent": USERAGENT},
        timeout=VECTOR_TIMEOUT,
    )
    check_response(response, "delete product")
