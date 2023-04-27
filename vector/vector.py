from __future__ import annotations

import json
from copy import deepcopy
from typing import Callable, List, Optional, Union

import descarteslabs as dl
import geojson
import ipyleaflet
from descarteslabs.common.property_filtering import GenericProperties

from .features import add as add_features
from .features import delete as delete_features
from .features import get as get_feature
from .features import query as query_features
from .features import update as update_features
from .products import create as create_product
from .products import delete as delete_product
from .products import get as get_product
from .products import list as list_products
from .products import update as update_product
from .tiles import create_layer


class FeatureCollection:
    """
    A class for creating and interacting with collections of features.
    """

    def __init__(
        self,
        feature_collection: Union[dict, geojson.FeatureCollection],
        parent_table: Table = None,
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
            self.feature_collection = geojson.FeatureCollection(feature_collection)

        self.parent = parent_table

    def __str__(self):
        """
        Simple string representation

        Returns
        -------
        str: str
            String representation
        """
        num_features = len(self.feature_collection["features"])
        return f"{num_features} from {self.parent}"

    def __repr__(self):
        """
        String representation

        Returns
        -------
        json: str
            JSON represetnation of this instance
        """
        return json.dumps(self.feature_collection)

    def filter(self, filter_func: Callable[[dict], bool]):
        """
        Create a new FeatureCollection by filtering this one. Note this filtering is performed
        on data that have *already* been pulled from the server. Where possible filtering
        should be performed with Table.query.

        Parameters
        ----------
        filter_func: Callable[[dict], bool]
            Preciate for selecting features.

        Returns
        -------
        filtered: FeatureCollection
            New FeatureCollection instance derived from filtering this one.
        """
        new_fc = deepcopy(self.feature_collection)
        new_fc["features"] = list(filter(filter_func, new_fc["features"]))
        return FeatureCollection(new_fc, self.parent)

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
        # Note that it is possible that there is no such element here, in which case

        try:
            return next(
                filter(
                    lambda x: x["uuid"] == feature_id,
                    self.feature_collection["features"],
                )
            )
        except StopIteration:
            # Raise a more user-friendly exception
            raise KeyError(f"Could not find {feature_id} in this FeatureCollection")

    def features(self):
        """
        Return the feature list

        Returns
        -------
        features: [GeoJSON Feature]
            Contained Features
        """
        return self.feature_collection["features"]


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
        return Table(get_product(product_id))

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

        return Table(create_product(product_id, *args, **kwargs))

    @staticmethod
    def list(tags: Union[List[str], None] = None) -> List[Table]:
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
        return [Table(d) for d in list_products(tags=tags)]

    def __init__(self, table_parameters: Union[dict, str]):
        """
        Initialize a Table instance -- users should create a Table instance via `Table.get` or `Table.create`

        Parameters
        ----------
        product_parameters: Union[dict, str]
            Dictionary of product parameters or the produt id.
        """
        if isinstance(table_parameters, str):
            table_parameters = get_product(table_parameters)

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
        self.parameters = update_product(self.parameters["id"], *args, **kwargs)

    def query(
        self, property_filter: GenericProperties = None, aoi: dict = None
    ) -> FeatureCollection:
        """
        Query features in a vector product.

        Parameters
        ----------
        property_filter : GenericProperties, optional
            Property filters to filter the product with.
        aoi : dict, optional
            A GeoJSON Feature to filter the vector product with.

        Returns
        -------
        features: FeatureCollection
            A FeatureCollection of the queried features.
        """
        return FeatureCollection(
            query_features(
                self.parameters["id"], property_filter=property_filter, aoi=aoi
            )
        )

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

        # Strip out any UUIDs, as they will be set by the call to add_features
        new_fc = deepcopy(feature_collection)

        for f in new_fc["features"]:
            f.pop("uuid", None)

        return FeatureCollection(add_features(self.parameters["id"], new_fc))

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
        return get_feature(self.parameters["id"], feature_id)

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
        return update_features(self.parameters["id"], feature_id, feature)

    def delete_feature(self, feature_id: str):
        """
        Delete a feature in a vector product.

        Parameters
        ----------
        feature_id : str
            ID of the feature.
        """
        delete_features(self.parameters["id"], feature_id)

    def visualize(
        self,
        name: str,
        map: ipyleaflet.leaflet.Map,
        property_filter: Optional[GenericProperties] = None,
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
        property_filter : GenericProperties, optional
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
        delete_product(self.parameters["id"])
