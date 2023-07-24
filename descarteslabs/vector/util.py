import json

import backoff
import requests

from .vector_exceptions import (
    ClientException,
    GenericException,
    RedirectException,
    ServerException,
)


def check_response(response: requests.Response, action: str):
    """
    Raise a meaninful Exception in response to a client error.
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
        server_error_msg = f'"{server_error_msg}"'
    except Exception:
        server_error_msg = ""

    error_msg = f'"{action}" failed due to {status_code_class} error {server_error_msg}'

    raise exception_type(error_msg)


backoff_wrapper = backoff.on_exception(
    backoff.expo,
    (requests.exceptions.RequestException, RedirectException, ServerException),
    max_tries=10,
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
