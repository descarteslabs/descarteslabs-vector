import os

import descarteslabs as dl

API_HOST = os.getenv(
    "VECTOR_API_HOST", "https://vector.appsci-production.aws.descarteslabs.com"
)


def get_token():
    """Get a JWT that can be used to authenticate with the Vector backend.

    The environment is checked first for a JWT. If not found, a token is generated.

    Returns
    -------
    token : str
        JWT that can be used to authenticate with the Vector backend.
    """
    return dl.auth.Auth.get_default_auth().token
