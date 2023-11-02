import io
import json
from typing import Union

import backoff
import geopandas as gpd
import pandas as pd
import requests

from .vector_exceptions import (
    ClientException,
    GenericException,
    RedirectException,
    ServerException,
)


def response_to_dataframe(
    response: requests.Response,
) -> Union[pd.DataFrame, gpd.GeoDataFrame]:
    """
    Function to convert the content of a response to
    Pandas DataFrame of GeoPandas GeoDataFrame.

    Parameters
    ----------
    response: requests.Response
        Response object from requests call.

    Returns
    -------
    Union[pd.DataFrame, gpd.GeoDataFrame]
    """
    buffer = io.BytesIO(response.content)

    is_spatial = response.headers.get("is_spatial")

    if is_spatial == "True":
        df = gpd.read_parquet(buffer)
    elif is_spatial == "False":
        df = pd.read_parquet(buffer)
    else:
        msg = "'response_to_dataframe' failed! File not from Vector API!"
        raise ServerException(msg)

    return df


def check_response(response: requests.Response, action: str):
    """
    Raise a meaningful Exception in response to a client error.
    """
    if response.status_code == 200:
        return

    status_code_class, exception_type = {
        3: ("redirect", RedirectException),
        4: ("client", ClientException),
        5: ("server", ServerException),
    }.get(response.status_code // 100, ("Unknown", GenericException))

    try:
        server_error_msg = json.loads(response.content.decode("utf-8"))["detail"]
        server_error_msg = f"'{server_error_msg}'"
    except Exception:
        server_error_msg = ""

    error_msg = f"'{action}' failed due to {status_code_class} error {server_error_msg}"

    raise exception_type(error_msg)


backoff_wrapper = backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException, RedirectException, ServerException),
    max_tries=3,
    jitter=backoff.full_jitter,
)

backoff_wrapper.__doc__ = """
A decorator to support backoffs in the vector client. This decorator
is a pass through to the `backoff.on_exception` method with a number
of preset parameters.

Specifically, this method applies an exponential backoff with jitter,
supporting 10 attempts. We handle the following exceptions:

- `requests.exceptions.RequestException`: Most functions call requests, which
  can raise this exception of a subclass thereof.
- `RedirectException`, `ServerException`: These are reaised by the `check_response`
  code in response to requests completing successfully, but returning a server-side
  error code.

Parameters
----------
target: Callable
    Function to decorate

Returns
-------
decorated-target: Callable
    Decorated function
"""
