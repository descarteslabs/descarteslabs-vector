from typing import List, Optional, Union

import requests

from .common import API_HOST, get_token
from .util import check_response


def _check_tags(tags: Union[List[str], None] = None):
    if tags:
        for tag in tags:
            if tag.find(",") >= 0:
                raise Exception('tags cannot contain ","')


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
        The input dictionary with keys containing None values removed.
    """
    return {k: v for k, v in d.items() if v is not None}


def create(
    product_id: str,
    name: str,
    description: Optional[str] = None,
    tags: Optional[List[str]] = None,
    readers: Optional[List[str]] = None,
    writers: Optional[List[str]] = None,
    owners: Optional[List[str]] = None,
) -> dict:
    """Create a vector product.

    Parameters
    ----------
    product_id : str
        ID of the vector product.
    name : str
        Name of the vector product.
    description : str, optional
        Description of the vector product.
    tags : list of str, optional
        A list of tags to associate with the vector product.
    readers : list of str, optional
        A list of vector product readers. Can take the form "user:{namespace}", "group:{group}", "org:{org}", or
        "email:{email}".
    writers : list of str, optional
        A list of vector product writers. Can take the form "user:{namespace}", "group:{group}", "org:{org}", or
        "email:{email}".
    owners : list of str, optional
        A list of vector product owners. Can take the form "user:{namespace}", "group:{group}", "org:{org}", or
        "email:{email}".

    Returns
    -------
    dict
        Details of the created vector product.
    """
    _check_tags(tags)

    response = requests.post(
        f"{API_HOST}/products/",
        headers={"Authorization": get_token()},
        json=_strip_null_values(
            {
                "id": product_id,
                "name": name,
                "description": description,
                "tags": tags,
                "readers": readers,
                "writers": writers,
                "owners": owners,
            }
        ),
    )
    check_response(response, "create product")
    return response.json()


def list(tags: Union[List[str], None] = None) -> List[dict]:
    """List vector products.

    Parameters
    ----------
    tags: List[str]
        Optional list of tags a table must have to be included in the returned list.

    Returns
    -------
    list of dict
        A list containing the details of each vector product where you have at least read access.
    """
    _check_tags(tags)

    if tags:
        response = requests.get(
            f"{API_HOST}/products/",
            headers={"Authorization": get_token()},
            params={"tags": ",".join(tags)},
        )
    else:
        response = requests.get(
            f"{API_HOST}/products/", headers={"Authorization": get_token()}
        )
    check_response(response, "list products")
    return response.json()


def get(product_id: str) -> dict:
    """Get a vector product.

    Parameters
    ----------
    product_id : str
        ID of the vector product.

    Returns
    -------
    dict
        Details of the vector product.
    """
    response = requests.get(
        f"{API_HOST}/products/{product_id}",
        headers={"Authorization": get_token()},
    )
    check_response(response, "get product")
    return response.json()


def update(
    product_id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    tags: Optional[List[str]] = None,
    readers: Optional[List[str]] = None,
    writers: Optional[List[str]] = None,
    owners: Optional[List[str]] = None,
) -> dict:
    """Update a vector product.

    Parameters
    ----------
    product_id : str
        ID of the vector product.
    name : str
        Name of the vector product.
    description : str, optional
        Description of the vector product.
    tags : list of str, optional
        A list of tags to associate with the vector product.
    readers : list of str, optional
        A list of vector product readers. Can take the form "user:{namespace}", "group:{group}", "org:{org}", or
        "email:{email}".
    writers : list of str, optional
        A list of vector product writers. Can take the form "user:{namespace}", "group:{group}", "org:{org}", or
        "email:{email}".
    owners : list of str, optional
        A list of vector product owners. Can take the form "user:{namespace}", "group:{group}", "org:{org}", or
        "email:{email}".

    Returns
    -------
    dict
        Details of the vector product.
    """
    _check_tags(tags)
    response = requests.patch(
        f"{API_HOST}/products/{product_id}",
        headers={"Authorization": get_token()},
        json=_strip_null_values(
            {
                "name": name,
                "description": description,
                "tags": tags,
                "readers": readers,
                "writers": writers,
                "owners": owners,
            }
        ),
    )
    check_response(response, "update product")
    return response.json()


def delete(product_id: str):
    """Delete a vector product.

    Parameters
    ----------
    product_id : str
        ID of the vector product.
    """
    response = requests.delete(
        f"{API_HOST}/products/{product_id}",
        headers={"Authorization": get_token()},
    )
    check_response(response, "delete product")
