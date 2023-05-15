import json

import requests


def check_response(response: requests.Response, action: str):
    """
    Raise a meaninful Exception in response to a client error.
    """
    if response.status_code == 200:
        return

    status_code_class = {
        3: "redirect",
        4: "client",
        5: "server",
    }.get(response.status_code // 100, "Unknown")

    try:
        server_error_msg = json.loads(response.content.decode("utf-8"))["detail"]
        server_error_msg = f'"{server_error_msg}"'
    except Exception:
        server_error_msg = ""

    error_msg = f'"{action}" failed due to {status_code_class} error {server_error_msg}'

    raise Exception(error_msg)
