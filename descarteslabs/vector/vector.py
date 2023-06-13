from __future__ import annotations

import json
from copy import deepcopy
from typing import Callable, List, Optional, Union

import descarteslabs as dl
import geojson
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


class Feature:
    def __init__(self, parameters: dict, parent_table: Table):
        """
        Initialize a new Feature instance. This should not be used directly.

        Parameters
        ----------
        parameters: dict
            JSON representation of this feature.
        parent_table: Table
            Optional parent table
        """
        assert isinstance(parameters, dict)
        assert parameters["geometry"]["type"] in accepted_geom_types
        assert parent_table
        self.parameters = parameters
        self.parent_table = parent_table

    def __str__(self):
        """
        Simple string representation

        Returns
        -------
        s: str
            Simple string representation
        """
        return f"{self.parent_table}:{self.parameters['uuid']}"

    def __repr__(self):
        """
        String representation

        Returns
        -------
        r: str
            String representation
        """
        return f"{self.parent_table}:{json.dumps(self.parameters, sort_keys=True)}"

    def set_properties(self, properties: dict):
        """
        Set properties for this Feature

        Parameters
        ----------
        properties: dict
            New properties for this feature
        """
        self.parameters["properties"] = properties

    def update(self):
        """
        Update this feature
        """
        x = deepcopy(self.parameters)
        x.pop("uuid", None)
        features_update(self.parent_table.parameters["id"], self.parameters["uuid"], x)

    def delete(self):
        """
        Delete this feature
        """
        features_delete(self.parent_table.parameters["id"], self.parameters["uuid"])

    def properties(self) -> dict:
        """
        Get properties

        Returns
        -------
        properties: dict
            Feature properties
        """
        return deepcopy(self.parameters["properties"])

    def geometry(self) -> dict:
        """
        Get geometry

        Returns
        -------
        geometry: dict
            Feature geometry
        """
        return deepcopy(self.parameters["geometry"])

    def uuid(self) -> dict:
        """
        Get uuid

        Returns
        -------
        uuid: uuid
            Feature UUID
        """
        return self.parameters["uuid"]


class FeatureCollection:
    """
    A class for creating and interacting with collections of features.
    """

    def __init__(
        self,
        feature_collection: Union[dict, geojson.FeatureCollection],
        parent_table: Table,
    ):
        """
        Initialize a FeatureCollection instance.

        Parameters
        ----------
        feature_collection: Union[GeoJSON.FeatureCollection, dict]
            GeoJSON feature collection
        parent_table: Table
            Optional Table instance for parent table, if there is one.
        """
        # The geojson.FeatureCollection constructor is not idempotent ...
        if issubclass(type(feature_collection), geojson.FeatureCollection):
            self.feature_collection = feature_collection
        else:
            # ... if it were, this following line would be sufficient
            self.feature_collection = geojson.FeatureCollection(
                feature_collection["features"]
            )

        assert parent_table

        self.parent_table = parent_table
        self.feature_list = self.feature_collection["features"]

    def __str__(self) -> str:
        """
        Simple string representation

        Returns
        -------
        str: str
            String representation
        """
        num_features = len(self.feature_collection["features"])
        return f"{num_features} from {self.parent_table}"

    def __repr__(self) -> str:
        """
        String representation

        Returns
        -------
        json: str
            JSON represetnation of this instance
        """
        return json.dumps(self.feature_collection, sort_keys=True)

    def filter(self, filter_func: Callable[[Feature], bool]):
        """
        Create a new FeatureCollection by filtering this one. Note this filtering is performed
        on data that have *already* been pulled from the server. Where possible filtering
        should be performed with a FeatureSearch instance.

        Parameters
        ----------
        filter_func: Callable[[Feature], bool]
            Preciate for selecting features.

        Returns
        -------
        filtered: FeatureCollection
            New FeatureCollection instance derived from filtering this one.
        """
        new_fc = deepcopy(self.feature_collection)
        new_fc["features"] = list(
            filter(
                lambda f: filter_func(Feature(f, self.parent_table)), new_fc["features"]
            )
        )
        return FeatureCollection(new_fc, self.parent_table)

    def get_feature(self, feature_id: str):
        """
        Get a specific feature from this FeatureCollection instance. This call requires that a
        feature collection was generated with feature-ID's. This occures, e.g., when the
        FeatureCollection instance is generated with `Table.query`.

        Parameters
        ----------
        feature_id: str
            Feature ID for which we would like the feature

        Retruns
        -------
        dict
            A GeoJSON Feature.
        """

        try:
            return Feature(
                next(
                    filter(
                        lambda x: x["uuid"] == feature_id,
                        self.feature_list,
                    )
                ),
                self.parent_table,
            )
        except StopIteration:
            # Raise a more user-friendly exception
            raise KeyError(f'Could not find "{feature_id}" in this FeatureCollection')

    def features(self) -> List[Feature]:
        """
        Return the feature list

        Returns
        -------
        features: [Feature]
            Contained Features
        """
        return [Feature(feature, self.parent_table) for feature in self.features_list()]

    def features_list(self) -> List[dict]:
        """
        Return the feature list as GeoJSON features

        Returns
        -------
        features: [GeoJSON Feature]
            Contained GeoJSON features
        """
        return deepcopy(self.feature_list)


# Supporting functions for geometry filtering.


def _geojson_to_shape(gj: dict) -> shapely.geometry.base.BaseGeometry:
    """
    Convert a GeoJSON dict into a shapely shape

    Parameters
    ----------
    gj: dict
        GeoJSON objct

    Returns
    -------
    shp: shapely.geometry.base.BaseGeometry
        Shapely shape for the geojson
    """
    return shapely.geometry.shape(gj)


def _dl_aoi_to_shape(aoi: dl.geo.GeoContext) -> shapely.geometry.base.BaseGeometry:
    """
    Convert a DL AOI object into a shapely shape

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
    Attempt to convert input to a shapely object. Raise an excpetion for non-None values that
    can't be converted.

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
        raise Exception(f'"{aoi}" not recognized as an aoi')

    return aoi


def _shape_to_geojson(shp: shapely.geometry.base.BaseGeometry) -> dict:
    """
    Convert a shapely object into a geojson

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
        Initialize an instance of FeatureSearch. Note instances of FeatureSearch should
        be generated with `Table.features`

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
        Create a new FeatureSearch instance that downselects to features that
        intersect the given AOI.

        Parameters
        ----------
        aoi: descarteslabs.geo.GeoContext
            AOI used to filter features by intersection

        Retuns
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
        Create a new FeatureSearch instance that downselects to features that
        are selected by the filter.

        Parameters
        ----------
        filter: descarteslabs.common.Properties
            AOI used to filter features by intersection

        Retuns
        -------
        feature_search: FeatureSearch
            New FeatureSearch instance that downselects to features in the AOI.
        """
        if self.property_filter:
            new_filter = self.property_filter & filter
        else:
            new_filter = filter

        return FeatureSearch(self.parent_table, self.aoi, new_filter)

    def collect(self) -> FeatureCollection:
        """
        Return a FeatureCollection with the selected items

        Returns
        -------
        fc: FeatureCollection
            Selected features as a FeatureCollection
        """
        return FeatureCollection(
            features_query(
                self.parent_table.parameters["id"],
                property_filter=self.property_filter,
                aoi=_shape_to_geojson(self.aoi),
            ),
            self.parent_table,
        )


class Table:
    """
    A class for creating and interacting with vector products.
    """

    @staticmethod
    def get(product_id: str) -> Table:
        """
        Get a Table instance associated with a product id. Raise an exception if
        this product_id doesn't exit.

        Parameters
        ----------
        product_id: str
            ID of product

        Returns
        -------
        table: Table
            Table instance for the product id.
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
            raise Exception(f'A table with id "{product_id}" already exists')

        return Table(products_create(product_id, *args, **kwargs))

    @staticmethod
    def list(tags: Optional[List[str]] = None) -> List[Table]:
        """
        List available vector products

        Parameters
        ----------
        tags: List[str]
            Optional list of tags a table must have to be returned.

        Returns
        -------
        products: list[Table]
            List of table instances.
        """
        return [Table(d) for d in products_list(tags=tags)]

    def __init__(self, table_parameters: Union[dict, str]):
        """
        Initialize a Table instance -- users should create a Table instance via `Table.get` or `Table.create`

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
        Generate a string representation of this Table instance

        Return
        ------
        json: str
            JSON representation of this table.
        """
        return json.dumps(self.parameters)

    def __str__(self):
        """
        Generate a name for this

        Return
        ------
        name: str
            Table name
        """
        return self.parameters["name"]

    def name(self):
        """
        Return the name of the table

        Returns
        -------
        name: str
            Table name
        """
        return self.parameters["name"]

    def tid(self):
        """
        Return the id of the table

        Returns
        -------
        tid: str
            Table ID
        """
        return self.parameters["id"]

    def description(self):
        """
        Return the description of the table

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
        Return a filterable FeatureSearch object

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
        feature_collection: Union[dict, FeatureCollection],
    ) -> FeatureCollection:
        """
        Add a feature collection to this table.

        Parameters
        ----------
        feature_collection: Union[dict, geojson.FeatureCollection, FeatureCollection]
            Collection of features to add to this table.

        Returns
        -------
        feature_collection: FeatureCollection
            Added features. Note that this will differ from the input in that the this will have
            feature IDs.
        """

        # Extract a geojson.FeatureCollection
        if issubclass(type(feature_collection), FeatureCollection):
            feature_collection = feature_collection.feature_collection

        # Strip out any UUIDs, as they will be set by the call to features_add, ensure that
        # geometry types
        new_fc = deepcopy(feature_collection)

        for f in new_fc["features"]:
            f.pop("uuid", None)
            geom_type = f["geometry"]["type"]
            if geom_type not in accepted_geom_types:
                raise Exception(
                    f'Vector doesn\'t support the "{geom_type}" geometry type'
                )

        return FeatureCollection(features_add(self.parameters["id"], new_fc), self)

    def get_feature(self, feature_id: str) -> dict:
        """
        Get a specific feature from this Table instance

        Parameters
        ----------
        feature_id: str
            Feature ID for which we would like the feature

        Retruns
        -------
        dict
            A GeoJSON Feature.
        """
        return features_get(self.parameters["id"], feature_id)

    def update_feature(self, feature_id: str, feature: dict) -> dict:
        """
        Update a feature in a vector product.

        Parameters
        ----------
        feature_id : str
            ID of the feature.
        feature : dict
            The GeoJSON Feature to replace the feature with.

        Returns
        -------
        dict
            A GeoJSON feature.
        """
        return features_update(self.parameters["id"], feature_id, feature)

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
            Vector tile styles to apply. See https://ipyleaflet.readthedocs.io/en/latest/layers/vector_tile.html for more
            details.

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
        Delete this vector product. This function will disable all subsequent non-static method calls.
        """
        products_delete(self.parameters["id"])
