import os

import descarteslabs as dl
import geopandas as gpd
import pandas as pd

from . import __version__

API_HOST = os.getenv("VECTOR_API_HOST", "https://vector.descarteslabs.com")
TYPES = (gpd.GeoDataFrame, pd.DataFrame)
VECTOR_TIMEOUT = int(os.environ.get("VECTOR_TIMEOUT", "600"))
USERAGENT = f"dl-vector/{__version__}"


def get_token() -> str:
    """
    Get a JWT that can be used to authenticate with the Vector backend.

    The environment is checked first for a JWT. If not found, a token is generated.

    Returns
    -------
    str
    """
    return dl.auth.Auth.get_default_auth().token
