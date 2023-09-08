from ipyleaflet import VectorTileLayer
from traitlets import Dict


class DLVectorTileLayer(VectorTileLayer):
    """
    A minimal wrapper around VectorTileLayer to add fetch_options
    """

    fetch_options = Dict({"credentials": "include"}).tag(sync=True, o=True)
