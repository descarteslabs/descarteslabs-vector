import json

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
