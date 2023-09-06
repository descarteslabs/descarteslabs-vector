from __future__ import annotations

import json
from typing import List, Optional, Union

import descarteslabs as dl
import geopandas as gpd
import ipyleaflet
import shapely
from descarteslabs.utils import Properties

# To avoid confusion we import these as <module>_<function>
from .features import add as features_add
from .features import delete as features_delete
from .features import get as features_get
from .features import query as features_query
from .features import update as features_update
from .products import create as products_create
from .products import delete as products_delete
from .products import get as products_get
from .products import list as products_list
from .products import update as products_update
from .tiles import create_layer
from .vector_exceptions import ClientException

accepted_geom_types = [
    "Point",
    "MultiPoint",
    "Line",
    "LineString",
    "MultiLine",
    "MultiLineString",
    "Polygon",
    "MultiPolygon",
    "GeometryCollection",
]

# Supporting functions for geometry filtering.


def _geojson_to_shape(gj: dict) -> shapely.geometry.base.BaseGeometry:
    """
    Convert a GeoJSON dict into a shapely shape.

    Parameters
    ----------
    gj: dict
        GeoJSON object

    Returns
    -------
    shp: shapely.geometry.base.BaseGeometry
        Shapely shape for the geojson
    """
    return shapely.geometry.shape(gj)


def _dl_aoi_to_shape(aoi: dl.geo.GeoContext) -> shapely.geometry.base.BaseGeometry:
    """
    Convert an AOI object into a shapely shape.

    Parameters
    ----------
    aoi: descarteslabs.geo.GeoContext
        AOI for which we want a shapely shape

    Returns
    -------
    shp: shapely.geometry.base.BaseGeometry
        Shapely shape for this AOI.
    """
    # h/t to the Savage M for this:
    return aoi.geometry or shapely.geometry.box(*list(aoi.bounds))


def _to_shape(
    aoi: Optional[
        Union[dl.geo.GeoContext, dict, shapely.geometry.base.BaseGeometry]
    ] = None
) -> Union[shapely.geometry.base.BaseGeometry, None]:
    """
    Attempt to convert input to a shapely object.

    Raise an exception for non-None values that can't be converted.

    Parameters
    ----------
    aoi: Optional[Union[dl.geo.GeoContext, dict, shapely.geometry.base.BaseGeometry]]
        Optinal aoi to convert to a shapely object

    Returns
    -------
    shp: Union[shapely.geometry.base.BaseGeometry, None]
        None if aoi is None, or a shapely representation of he aoi
    """

    if not aoi:
        return None

    # Convert the AOI object to a shapely object so we can
    # perform intersections.
    if isinstance(aoi, dict):
        aoi = _geojson_to_shape(aoi)
    elif issubclass(type(aoi), dl.geo.GeoContext):
        aoi = _dl_aoi_to_shape(aoi)
    elif issubclass(type(aoi), shapely.geometry.base.BaseGeometry):
        return aoi
    else:
        raise ClientException(f'"{aoi}" not recognized as an aoi')

    return aoi


def _shape_to_geojson(shp: shapely.geometry.base.BaseGeometry) -> dict:
    """
    Convert a shapely object into a GeoJSON.

    Parameters
    ----------
    shp: shapely.geometry.base.BaseGeometry

    Returns
    -------
    gj: dict
        GeoJSON dict for this shape
    """
    if shp:
        return shapely.geometry.mapping(shp)
    return None


class FeatureSearch:
    """
    A class for searching and filtering through vector features
    """

    def __init__(
        self,
        parent_table: str,
        aoi: Optional[
            Union[dl.geo.GeoContext, dict, shapely.geometry.base.BaseGeometry]
        ] = None,
        filter: Optional[Properties] = None,
    ):
        """
        Initialize an instance of FeatureSearch.

        Note instances of FeatureSearch should be generated with `Table.features`.

        Parameters
        ----------
        parent_table: str
            Table ID for the parent table.
        """
        self.parent_table = parent_table
        self.aoi = _to_shape(aoi)
        self.property_filter = filter

        # Setting this to True means successive calls to `intersects` will "remember"
        # previous invocations of `intersects`.
        self.accumulate_intersections = False

    def intersects(
        self, aoi: Union[dl.geo.GeoContext, dict, shapely.geometry.base.BaseGeometry]
    ) -> FeatureSearch:
        """
        Create a new FeatureSearch instance that downselects to features that intersect the given AOI.

        Parameters
        ----------
        aoi: descarteslabs.geo.GeoContext
            AOI used to filter features by intersection

        Returns
        -------
        feature_search: FeatureSearch
            New FeatureSearch instance that downselects to features in the AOI.
        """
        if self.aoi and self.accumulate_intersections:
            new_aoi = self.aoi.intersection(_to_shape(aoi))
        else:
            new_aoi = _to_shape(aoi)

        return FeatureSearch(self.parent_table, new_aoi, self.property_filter)

    def filter(self, filter: Properties) -> FeatureSearch:
        """
        Create a new FeatureSearch instance that downselects to features that are selected by the filter.

        Parameters
        ----------
        filter: descarteslabs.common.Properties
            AOI used to filter features by intersection

        Returns
        -------
        feature_search: FeatureSearch
            New FeatureSearch instance that downselects to features in the AOI.
        """
        if self.property_filter:
            new_filter = self.property_filter & filter
        else:
            new_filter = filter

        return FeatureSearch(self.parent_table, self.aoi, new_filter)

    def collect(self) -> gpd.GeoDataFrame:
        """
        Return a GeoPandas dataframe with the selected items.

        Returns
        -------
        gpd.GeoDataFrame:
            A GeoPandas dataframe.
        """
        return features_query(
            self.parent_table.parameters["id"],
            property_filter=self.property_filter,
            aoi=_shape_to_geojson(self.aoi),
        )


class Table:
    """
    A class for creating and interacting with vector products.
    """

    @staticmethod
    def get(product_id: str) -> Table:
        """
        Get a Table instance associated with a product id. Raise an exception if this `product_id` doesn't exit.

        Parameters
        ----------
        product_id: str
            ID of product

        Returns
        -------
        table: Table
            Table instance for the product ID.
        """
        return Table(products_get(product_id))

    @staticmethod
    def create(product_id, *args, **kwargs) -> Table:
        """
        Create a vector product.

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
        model : VectorBaseModel, optional
            A model that provides a user provided schema for the vector table.

        Returns
        -------
        table: Table
            Table instace representing new product
        """

        prefix = dl.auth.Auth().payload["org"]

        try:
            Table.get(f"{prefix}:{product_id}")
            table_exists = True
        except Exception:
            table_exists = False

        if table_exists:
            raise ClientException(f'A table with id "{product_id}" already exists')

        return Table(products_create(product_id, *args, **kwargs))

    @staticmethod
    def list(tags: Optional[List[str]] = None) -> List[Table]:
        """
        List available vector products.

        Parameters
        ----------
        tags: list of str
            Optional list of tags a table must have to be returned.

        Returns
        -------
        products: list of Table
            List of table instances.
        """
        return [Table(d) for d in products_list(tags=tags)]

    def __init__(self, table_parameters: Union[dict, str]):
        """
        Initialize a Table instance.

        Users should create a Table instance via `Table.get` or `Table.create`.

        Parameters
        ----------
        product_parameters: Union[dict, str]
            Dictionary of product parameters or the produt id.
        """
        if isinstance(table_parameters, str):
            table_parameters = products_get(table_parameters)

        self.parameters = table_parameters

    def __repr__(self):
        """
        Generate a string representation of this Table instance.

        Return
        ------
        json: str
            JSON representation of this table.
        """
        return json.dumps(self.parameters)

    def __str__(self):
        """
        Generate a name for this.

        Return
        ------
        name: str
            Table name
        """
        return self.parameters["name"]

    def name(self):
        """
        Return the name of the table.

        Returns
        -------
        name: str
            Table name
        """
        return self.parameters["name"]

    def tid(self):
        """
        Return the ID of the table.

        Returns
        -------
        tid: str
            Table ID
        """
        return self.parameters["id"]

    def description(self):
        """
        Return the description of the table.

        Returns
        -------
        description: str
            Table Description
        """
        return self.parameters["description"]

    def update(self, *args, **kwargs):
        """
        Update this vector product.

        Parameters
        ----------
        name : str
            New name of the vector product.
        description : str, optional
            New Description of the vector product.
        tags : list of str, optional
            New list of tags to associate with the vector product.
        readers : list of str, optional
            New list of vector product readers. Can take the form "user:{namespace}", "group:{group}", "org:{org}", or
            "email:{email}".
        writers : list of str, optional
            New list of vector product writers. Can take the form "user:{namespace}", "group:{group}", "org:{org}", or
            "email:{email}".
        owners : list of str, optional
            New list of vector product owners. Can take the form "user:{namespace}", "group:{group}", "org:{org}", or
            "email:{email}".
        """
        self.parameters = products_update(self.parameters["id"], *args, **kwargs)

    def features(
        self,
        aoi: Optional[
            Union[dl.geo.GeoContext, dict, shapely.geometry.base.BaseGeometry]
        ] = None,
        filter: Optional[Properties] = None,
    ) -> FeatureSearch:
        """
        Return a filterable FeatureSearch object.

        Parameters
        ----------
        aoi: Optional[Union[dl.geo.GeoContext, dict, shapely.geometry.base.BaseGeometry]]
            Optional AOI object on which to filter features.
        filter: Optional[Properties]
            Optional property filte.

        Returns
        -------
        fs: FeatureSearch
            Filteratble object
        """
        return FeatureSearch(self, aoi=aoi, filter=filter)

    def add(
        self,
        dataframe: gpd.GeoDataFrame,
    ) -> gpd.GeoDataFrame:
        """
        Add a GeoPandas dataframe to this table.

        Parameters
        ----------
        dataframe:gpd.GeoDataFrame
            GeoPandas dataframe to add to this table.

        Returns
        -------
        dataframe: gpd.GeoDataFrame
            Added features. Note that this will differ from the input in that UUIDs have been attributed.
        """

        return features_add(self.parameters["id"], dataframe)

    def get_feature(self, feature_id: str) -> gpd.GeoDataFrame:
        """
        Get a specific feature from this Table instance.

        Parameters
        ----------
        feature_id: str
            Feature ID for which we would like the feature

        Retruns
        -------
        gpd.GeoDataFrame
            A GeoPandas dataframe.
        """
        return features_get(self.parameters["id"], feature_id)

    def try_get_feature(self, feature_id: str) -> Union[dict, None]:
        """
        Get a specific feature from this Table instance. If it isn't present, return None.

        Parameters
        ----------
        feature_id: str
            Feature ID for which we would like the feature

        Retruns
        -------
        gpd.GeoDataFrame
            A GeoPandas dataframe.
        """
        try:
            return features_get(self.parameters["id"], feature_id)
        except ClientException:
            return None

    def update_feature(
        self, feature_id: str, dataframe: gpd.GeoDataFrame
    ) -> gpd.GeoDataFrame:
        """
        Update a feature in a vector product.

        Parameters
        ----------
        feature_id : str
            ID of the feature.
        feature : dict
            The GeoPandas dataframe to replace the feature with.

        Returns
        -------
        gpd.GeoDataFrame
            A GeoPandas dataframe.
        """
        return features_update(self.parameters["id"], feature_id, dataframe)

    def delete_feature(self, feature_id: str):
        """
        Delete a feature in a vector product.

        Parameters
        ----------
        feature_id : str
            ID of the feature.
        """
        features_delete(self.parameters["id"], feature_id)

    def visualize(
        self,
        name: str,
        map: ipyleaflet.leaflet.Map,
        property_filter: Optional[Properties] = None,
        include_properties: Optional[List[str]] = None,
        vector_tile_layer_styles: Optional[dict] = None,
    ) -> ipyleaflet.leaflet.TileLayer:
        """
        Visualize this Table as an `ipyleaflet` vector tile layer.

        Parameters
        ----------
        name : str
            Name to give to the ipyleaflet vector tile layer.
        map: ipyleaflet.leaflet.Map
            Map to which to add the layer
        property_filter : Properties, optional
            Property filter to apply to the vector tiles.
        include_properties : list of str, optional
            Properties to include in the vector tiles. These can be used for styling.
        vector_tile_layer_styles : dict, optional
            Vector tile styles to apply. See https://ipyleaflet.readthedocs.io/en/latest/layers/vector_tile.html for
            more details.

        Returns
        -------
        ipyleaflet.VectorTileLayer
            Vector tile layer that can be added to an ipyleaflet map.
        """
        lyr = create_layer(
            self.parameters["id"],
            name,
            property_filter=property_filter,
            include_properties=include_properties,
            vector_tile_layer_styles=vector_tile_layer_styles,
        )
        map.add_layer(lyr)
        return lyr

    def delete(self):
        """
        Delete this vector product.

        This function will disable all subsequent non-static method calls.
        """
        products_delete(self.parameters["id"])
