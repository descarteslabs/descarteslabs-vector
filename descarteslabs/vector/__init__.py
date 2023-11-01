from importlib.metadata import version

__version__ = version("descarteslabs-vector")

from descarteslabs.utils import Properties

from .vector import Feature, Table, TableOptions

properties = Properties()

__all__ = [
    "Table",
    "TableOptions",
    "Feature",
    "properties",
]
