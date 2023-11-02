import json
import urllib.parse
from typing import List, Optional

from descarteslabs.utils import Properties

from .common import API_HOST
from .layers import DLVectorTileLayer


def create_layer(
    product_id: str,
    name: str,
    is_spatial: bool,
    property_filter: Optional[Properties] = None,
    columns: Optional[List[str]] = None,
    vector_tile_layer_styles: Optional[dict] = None,
) -> DLVectorTileLayer:
    """
    Create vector tile layer from a Vector Table.

    Parameters
    ----------
    product_id : str
        Product ID of the Vector Table.
    name : str
        Name to give to the ipyleaflet vector tile layer.
    is_spatial : bool
        Boolean indicating whether or not this data is spatial.
    property_filter : Properties, optional
        Property filter to apply to the vector tiles.
    columns : list of str, optional
       Optional list of column names to include. These can be used for styling.
    vector_tile_layer_styles : dict, optional
        Vector tile styles to apply. See https://ipyleaflet.readthedocs.io/en/latest/layers/vector_tile.html for more
        details.

    Returns
    -------
    DLVectorTileLayer
    """
    # Error if the table is not spatial
    if not is_spatial:
        raise TypeError(f"'{product_id}' is not a spatially enabled Vector Table!")

    # Initialize vector tile layer styles if no styles are provided
    if vector_tile_layer_styles is None:
        vector_tile_layer_styles = {}

    # Initialize the property filter if none is provided
    if property_filter is not None:
        property_filter = property_filter.serialize()

    # Construct the query parameters
    property_filter = json.dumps(property_filter)
    columns = json.dumps(columns)
    query_params = urllib.parse.urlencode(
        {
            "property_filter": property_filter,
            "columns": columns,
        },
        doseq=True,
    )

    # Create an ipyleaflet vector tile layer and return it
    lyr = DLVectorTileLayer(
        url=f"{API_HOST}/products/{product_id}/tiles/{{z}}/{{x}}/{{y}}?{query_params}",
        name=name,
        vector_tile_layer_styles=vector_tile_layer_styles,
    )
    return lyr
