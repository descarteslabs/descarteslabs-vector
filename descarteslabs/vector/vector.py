from __future__ import annotations

import json
from typing import List, Literal, Optional, Tuple, Union

import descarteslabs as dl
import geopandas as gpd
import ipyleaflet
import shapely
from descarteslabs.utils import Properties

# To avoid confusion we import these as <module>_<function>
from .features import Statistic
from .features import add as features_add
from .features import aggregate as features_aggregate
from .features import delete as features_delete
from .features import get as features_get
from .features import join as features_join
from .features import query as features_query
from .features import update as features_update
from .layers import DLVectorTileLayer
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


class TableOptions:
    """
    A class controling Table options and parameters.
    """

    def __init__(
        self,
        product_id: str,
        aoi: Optional[
            Union[dl.geo.GeoContext, dict, shapely.geometry.base.BaseGeometry]
        ] = None,
        property_filter: Optional[Properties] = None,
        columns: Optional[List[str]] = None,
    ):
        """
        Initialize an instance of TableOptions.

        Parameters
        ----------
        product_id: str
            Product ID of Vector Table.
        aoi: Optional[Union[dl.geo.GeoContext, dict, shapely.geometry.base.BaseGeometry]]
            AOI associated with this TableOptions.
        property_filter: Optional[Properties]
            Property filter associated with this TableOptions.
        columns: Optional[List[str]]
            List of columns to include in each query.
        """
        self._product_id = product_id
        self._aoi = _to_shape(aoi)
        self._property_filter = property_filter
        self._columns = columns

    @property
    def product_id(self) -> str:
        """
        Return the product ID associated with this TableOptions.
        Returns
        -------
        str
        """
        return self._product_id

    @product_id.setter
    def product_id(self, product_id: str) -> None:
        """
        Set the product ID associated with this TableOptions.
        Returns
        -------
        str
        """
        if not isinstance(product_id, str):
            raise TypeError("'product_id' must be of type <str>!")
        self._product_id = product_id

    @property
    def aoi(self) -> shapely.geometry.shape:
        """
        Return the aoi associated with this TableOptions.

        Returns
        -------
        shapely.geometry.shape
        """
        return self._aoi

    @aoi.setter
    def aoi(
        self,
        aoi: Optional[
            Union[dl.geo.GeoContext, dict, shapely.geometry.base.BaseGeometry]
        ] = None,
    ) -> None:
        """
        Set the aoi associated with this TableOptions.

        Returns
        -------
        None
        """
        self._aoi = _to_shape(aoi)

    @property
    def property_filter(self) -> Properties:
        """
        Return the property filter associated with this TableOptions.

        Returns
        -------
        Properties
        """
        return self._property_filter

    @property_filter.setter
    def property_filter(self, property_filter: Optional[Properties] = None) -> None:
        """
        Set the property_filter associated with this TableOptions.

        Returns
        -------
        None
        """
        if hasattr(property_filter, "jsonapi_serialize"):
            self._property_filter = property_filter
        elif not property_filter:
            self._property_filter = None
        else:
            raise TypeError("'property_filter' must be of type <None> or <Properties>!")

    @property
    def columns(self) -> List[str]:
        """
        Return the columns associated with this TableOptions.

        Returns
        -------
        list
        """
        return self._columns

    @columns.setter
    def columns(self, columns: Optional[List[str]] = None) -> None:
        """
        Set the columns associated with this TableOptions.

        Returns
        -------
        None
        """
        if isinstance(columns, list):
            self._columns = columns
        elif not columns:
            self._columns = None
        else:
            raise TypeError("'columns' must be of type <None> or <list>!")
        self._columns = columns


class Table:
    """
    A class for creating and interacting with Vector products.
    """

    def __init__(
        self, table_parameters: Union[dict, str], options: TableOptions = None
    ):
        """
        Initialize a Table instance.

        Users should create a Table instance via `Table.get` or `Table.create`.

        Parameters
        ----------
        product_parameters: Union[dict, str]
            Dictionary of product parameters or the product id.
        """
        if isinstance(table_parameters, str):
            table_parameters = products_get(table_parameters)

        for k, v in table_parameters.items():
            setattr(self, f"_{k}", v)

        if not options:
            options = TableOptions(self.id)

        if not isinstance(options, TableOptions):
            raise TypeError(("'options' must be of type <TableOptions>!"))
        self.options = options

    @staticmethod
    def get(
        product_id: str,
        aoi: Optional[
            Union[dl.geo.GeoContext, dict, shapely.geometry.base.BaseGeometry]
        ] = None,
        property_filter: Optional[Properties] = None,
        columns: Optional[List[str]] = [],
    ) -> Table:
        """
        Get a Table instance associated with a product id. Raise an exception if this `product_id` doesn't exit.

        Parameters
        ----------
        product_id: str
            Product ID of Vector Table.
        aoi: Optional[Union[dl.geo.GeoContext, dict, shapely.geometry.base.BaseGeometry]]
            AOI associated with this TableOptions.
        property_filter: Optional[Properties]
            Property filter associated with this TableOptions.
        columns: Optional[List[str]]
            List of columns to include in each query.

        Returns
        -------
        table: Table
            Table instance for the product ID.
        """
        options = TableOptions(
            product_id=product_id,
            aoi=aoi,
            property_filter=property_filter,
            columns=columns,
        )

        return Table(table_parameters=products_get(product_id), options=options)

    @staticmethod
    def create(product_id, *args, **kwargs) -> Table:
        """
        Create a Vector product.

        Parameters
        ----------
        product_id : str
            ID of the Vector product.
        name : str
            Name of the Vector product.
        description : str, optional
            Description of the Vector product.
        tags : list of str, optional
            A list of tags to associate with the Vector product.
        readers : list of str, optional
            A list of Vector product readers. Can take the form "user:{namespace}", "group:{group}", "org:{org}", or
            "email:{email}".
        writers : list of str, optional
            A list of Vector product writers. Can take the form "user:{namespace}", "group:{group}", "org:{org}", or
            "email:{email}".
        owners : list of str, optional
            A list of Vector product owners. Can take the form "user:{namespace}", "group:{group}", "org:{org}", or
            "email:{email}".
        model : VectorBaseModel, optional
            A model that provides a user provided schema for the Vector table.

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
        List available Vector products.

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

    @property
    def id(self) -> str:
        """
        Return the ID of the table.

        Returns
        -------
        id: str
            Table ID
        """
        return self._id

    @property
    def name(self) -> str:
        """
        Return the name of the table.

        Returns
        -------
        name: str
            Table name
        """
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        """
        Set the name of the table.

        Returns
        -------
        None
        """
        if isinstance(value, str):
            self._name = value
        else:
            raise ValueError("Table 'name' must be of type 'str'")

    @property
    def description(self) -> str:
        """
        Return the description of the table.

        Returns
        -------
        None
        """
        return self._description

    @description.setter
    def description(self, value: str) -> None:
        """
        Set the description of the table.

        Returns
        -------
        None
        """
        if isinstance(value, str):
            self._description = value
        else:
            raise ValueError("Table 'description' must be of type 'str'")

    @property
    def tags(self) -> list:
        """
        Return the tags of the table.

        Returns
        -------
        tags: list
            Table tags
        """
        return self._tags

    @tags.setter
    def tags(self, value: str) -> None:
        """
        Set the tags for the table.

        Returns
        -------
        None
        """
        if isinstance(value, list):
            self._tags = value
        else:
            raise ValueError("Table 'tags' must be of type 'list'")

    @property
    def readers(self) -> list:
        """
        Return the readers of the table.

        Returns
        -------
        readers: list
            Table readers
        """
        return self._readers

    @readers.setter
    def readers(self, value: list) -> None:
        """
        Set the readers for the table.

        Returns
        -------
        None
        """
        if isinstance(value, list):
            self._readers = value
        else:
            raise ValueError("Table 'readers' must be of type 'list'")

    @property
    def writers(self) -> list:
        """
        Return the writers of the table.

        Returns
        -------
        writers: list
            Table writers
        """
        return self._writers

    @writers.setter
    def writers(self, value: list) -> None:
        """
        Set the writers for the table.

        Returns
        -------
        None
        """
        if isinstance(value, list):
            self._writers = value
        else:
            raise ValueError("Table 'writers' must be of type 'list'")

    @property
    def owners(self) -> list:
        """
        Return the owners of the table.

        Returns
        -------
        owners: list
            Table owners
        """
        return self._owners

    @owners.setter
    def owners(self, value: list) -> None:
        """
        Set the owners for the table.

        Returns
        -------
        None
        """
        if isinstance(value, list):
            self._owners = value
        else:
            raise ValueError("Table 'owners' must be of type 'list'")

    @property
    def model(self) -> dict:
        """
        Return the model of the table.

        Returns
        -------
        model: dict
            Table model
        """
        return self._model

    @property
    def columns(self) -> list:
        """
        Return the column names of the table.

        Returns
        -------
        columns: list
            Table columns
        """
        return list(self._model["properties"].keys())

    @property
    def parameters(self) -> dict:
        """
        Return the table parameters as dictionary.

        Returns
        -------
        parameters: dict
            Table parameters
        """

        keys = [
            "_id",
            "_name",
            "_description",
            "_tags",
            "_readers",
            "_writers",
            "_owners",
            "_model",
        ]

        params = {}

        for k in keys:
            params[k.lstrip("_")] = self.__dict__[k]

        return params

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
        return self.name

    def save(self) -> None:
        """
        Save/update this Vector product.

        Parameters
        ----------
        name : str
            New name of the Vector product.
        description : str, optional
            New Description of the Vector product.
        tags : list of str, optional
            New list of tags to associate with the Vector product.
        readers : list of str, optional
            New list of Vector product readers. Can take the form "user:{namespace}", "group:{group}", "org:{org}", or
            "email:{email}".
        writers : list of str, optional
            New list of Vector product writers. Can take the form "user:{namespace}", "group:{group}", "org:{org}", or
            "email:{email}".
        owners : list of str, optional
            New list of Vector product owners. Can take the form "user:{namespace}", "group:{group}", "org:{org}", or
            "email:{email}".
        """
        products_update(
            product_id=self.id,
            name=self.name,
            description=self.description,
            tags=self.tags,
            readers=self.readers,
            writers=self.writers,
            owners=self.owners,
        )

    def add(
        self,
        dataframe: gpd.GeoDataFrame,
    ) -> FeatureCollection:
        """
        Add a GeoPandas dataframe to this table.

        Parameters
        ----------
        dataframe:gpd.GeoDataFrame
            GeoPandas dataframe to add to this table.

        Returns
        -------
        FeatureCollection
            Added features. Note that this will differ from the input in that UUIDs have been attributed.
        """

        return FeatureCollection(self.id, features_add(self.id, dataframe))

    def get_feature(self, feature_id: str) -> Feature:
        """
        Get a specific feature from this Table instance.

        Parameters
        ----------
        feature_id: str
            Feature ID for the feature to get.

        Returns
        -------
        Feature
            A Vector Feature instance.
        """
        return Feature.get(f"{self.id}:{feature_id}")

    def try_get_feature(self, feature_id: str) -> Union[Feature, None]:
        """
        Get a specific feature from this Table instance. If it isn't present, return None.

        Parameters
        ----------
        feature_id: str
            Feature ID for the feature to get.

        Returns
        -------
        Feature
            A Vector Feature instance.
        """
        try:
            return Feature.get(f"{self.id}:{feature_id}")
        except ClientException:
            return None

    def visualize(
        self,
        name: str,
        map: ipyleaflet.leaflet.Map,
        vector_tile_layer_styles: Optional[dict] = None,
        override_options: TableOptions = None,
    ) -> DLVectorTileLayer:
        """
        Visualize this Table as an `ipyleaflet` VectorTileLayer.

        Parameters
        ----------
        name : str
            Name to give to the ipyleaflet VectorTileLayer.
        map: ipyleaflet.leaflet.Map
            Map to which to add the layer
        vector_tile_layer_styles : dict, optional
            Vector tile styles to apply. See https://ipyleaflet.readthedocs.io/en/latest/layers/vector_tile.html for
            more details.
        override_options: TableOptions
            Override options for this query. AOI option is ignored
            when invoking this method.

        Returns
        -------
        DLVectorTileLayer
            Vector tile layer that can be added to an ipyleaflet map.
        """
        options = override_options if override_options else self.options

        if not isinstance(options, TableOptions):
            raise TypeError("'options' must be of type <TableOptions>.")

        lyr = create_layer(
            product_id=self.id,
            name=name,
            property_filter=options.property_filter,
            columns=options.columns,
            vector_tile_layer_styles={self.id: vector_tile_layer_styles},
        )
        for layer in map.layers:
            if layer.name == name:
                map.remove_layer(layer)
                break
        map.add_layer(lyr)
        return lyr

    def search(self, override_options: TableOptions = None) -> FeatureCollection:
        """
        Method to execute query and return a Vector FeatureCollection
        (GeoPandas dataframe) with the selected items.

        Parameters
        ----------
        override_options: TableOptions
            Override options for this query.

        Returns
        -------
        df: FeatureCollection
            A Vector FeatureCollection.
        """

        options = override_options if override_options else self.options

        if not isinstance(options, TableOptions):
            raise TypeError("'options' must be of type <TableOptions>.")

        df = features_query(
            options.product_id,
            property_filter=options.property_filter,
            aoi=_shape_to_geojson(options.aoi),
            columns=options.columns,
        )

        return FeatureCollection(options.product_id, df)

    def _join(
        self,
        join_table: [Union[Table, TableOptions]],
        join_type: Literal[
            "INNER", "LEFT", "RIGHT", "INTERSECTS", "CONTAINS", "OVERLAPS", "WITHIN"
        ],
        join_columns: List[Tuple[str, str]] = None,
        override_options: Optional[TableOptions] = None,
    ):
        """
        Private method to execute join or spatial join and return a Vector FeatureCollection
        (GeoPandas dataframe) with the selected items.

        Parameters
        ----------
        join_table: [Union[Table, TableOptions]]
            The table to join. Can be either Table of TableOptions.
        join_type: Literal["INNER", "LEFT", "RIGHT", "INTERSECTS", "CONTAINS", "OVERLAPS", "WITHIN"]
            The type of join to perform. Must be one of INNER,
            LEFT, RIGHT, INTERSECTS, CONTAINS, OVERLAPS, WITHIN.
        join_columns: List[Tuple[str, str]]
            List of column names to join on. Must be formatted
            as [(table1_col1, table2_col2), ...].
        override_options: TableOptions
            Override options for this query.

        Returns
        -------
        df: FeatureCollection
        """
        options = override_options if override_options else self.options

        if not isinstance(options, TableOptions):
            raise TypeError("'override_options' must be of type <TableOptions>.")

        if isinstance(join_table, TableOptions):
            pass
        elif isinstance(join_table, Table):
            join_table = join_table.options
        else:
            raise TypeError("'join_table' must be of type <TableOptions>.")

        include_columns = [tuple(options.columns), tuple(join_table.columns)]

        df = features_join(
            input_product_id=options.product_id,
            join_product_id=join_table.product_id,
            join_type=join_type,
            join_columns=join_columns,
            include_columns=include_columns,
            input_property_filter=options.property_filter,
            input_aoi=_shape_to_geojson(options.aoi),
            join_property_filter=join_table.property_filter,
            join_aoi=_shape_to_geojson(join_table.aoi),
        )

        return FeatureCollection(options.product_id, df)

    def join(
        self,
        join_table: [Union[Table, TableOptions]],
        join_type: Literal["INNER", "LEFT", "RIGHT"],
        join_columns: List[Tuple[str, str]] = None,
        override_options: Optional[TableOptions] = None,
    ) -> FeatureCollection:
        """
        Method to execute join and return a Vector FeatureCollection
        (GeoPandas dataframe) with the selected items.

        Parameters
        ----------
        join_table: [Union[Table, TableOptions]]
            The table to join. Can be either Table of TableOptions.
        join_type: Literal["INNER", "LEFT", "RIGHT"]
            The type of join to perform. Must be one of INNER,
            LEFT, or RIGHT.
        join_columns: List[Tuple[str, str]]
            List of column names to join on. Must be formatted
            as [(table1_col1, table2_col2), ...].
        override_options: TableOptions
            Override options for this query.

        Returns
        -------
        df: FeatureCollection
            A Vector FeatureCollection.
        """
        return self._join(
            join_table=join_table,
            join_type=join_type,
            join_columns=join_columns,
            override_options=override_options,
        )

    def sjoin(
        self,
        join_table: [Union[Table, TableOptions]],
        join_type: Literal["INTERSECTS", "CONTAINS", "OVERLAPS", "WITHIN"],
        override_options: Optional[TableOptions] = None,
    ) -> FeatureCollection:
        """
        Method to execute spatial join and return a Vector FeatureCollection
        (GeoPandas dataframe) with the selected items.

        Parameters
        ----------
        join_table: [Union[Table, TableOptions]]
            The table to join. Can be either Table of TableOptions.
        join_type: Literal["INTERSECTS", "CONTAINS", "OVERLAPS", "WITHIN"]
            The type of join to perform. Must be one of INTERSECTS,
            CONTAINS, OVERLAPS, WITHIN.
        override_options: TableOptions
            Override options for this query.

        Returns
        -------
        df: FeatureCollection
            A Vector FeatureCollection.
        """

        return self._join(
            join_table=join_table,
            join_type=join_type,
            override_options=override_options,
        )

    def _aggregate(
        self, statistic: Statistic, override_options: TableOptions
    ) -> Union[int, dict]:
        """
        Private method for handling aggregate functions.

        Parameters
        ----------
        statistic: Statistic
            Statistic to calculate.
        override_options: TableOptions
            Override options for this query.

        Returns
        -------
        Union[int, dict]
            The statistic COUNT will always return an integer. All
            other statistics will return a dictionary of results.
            Keys of the dictionary will be the column names requested
            appended with the statistic ('column_1.STATISTIC') and values
            are the result of the aggregate statistic.
        """
        options = override_options if override_options else self.options

        if not isinstance(statistic, Statistic):
            raise TypeError("'statistic' must be of type <Statistic>.")

        if not isinstance(options, TableOptions):
            raise TypeError("'options' must be of type <TableOptions>.")

        return features_aggregate(
            product_id=options.product_id,
            statistic=statistic,
            columns=options.columns,
            property_filter=options.property_filter,
            aoi=_shape_to_geojson(options.aoi),
        )

    def count(
        self,
        override_options: Optional[TableOptions] = None,
    ) -> int:
        """
        Method to return the row count of the vector product.

        Parameters
        ----------
        override_options: TableOptions
            Override options for this query.

        Returns
        -------
        int
        """

        return self._aggregate(Statistic.COUNT, override_options)

    def sum(
        self,
        override_options: Optional[TableOptions] = None,
    ) -> dict:
        """
        Method to return the row count of the vector product.

        Parameters
        ----------
        override_options: TableOptions
            Override options for this query.

        Returns
        -------
        dict :
            Dictionary of results. Keys the column names requested
            appended with the statistic ('column_1.SUM') and values
            are the result of the aggregate statistic.
        """

        return self._aggregate(Statistic.SUM, override_options)

    def min(
        self,
        override_options: Optional[TableOptions] = None,
    ) -> dict:
        """
        Method to return the row count of the vector product.

        Parameters
        ----------
        override_options: TableOptions
            Override options for this query.

        Returns
        -------
        dict :
            Dictionary of results. Keys the column names requested
            appended with the statistic ('column_1.MIN') and values
            are the result of the aggregate statistic.
        """

        return self._aggregate(Statistic.MIN, override_options)

    def max(
        self,
        override_options: Optional[TableOptions] = None,
    ) -> dict:
        """
        Method to return the row count of the vector product.

        Parameters
        ----------
        override_options: TableOptions
            Override options for this query.

        Returns
        -------
        dict :
            Dictionary of results. Keys the column names requested
            appended with the statistic ('column_1.MAX') and values
            are the result of the aggregate statistic.
        """

        return self._aggregate(Statistic.MAX, override_options)

    def mean(
        self,
        override_options: Optional[TableOptions] = None,
    ) -> dict:
        """
        Method to return the row count of the vector product.

        Parameters
        ----------
        override_options: TableOptions
            Override options for this query.

        Returns
        -------
        dict :
            Dictionary of results. Keys the column names requested
            appended with the statistic ('column_1.MEAN') and values
            are the result of the aggregate statistic.
        """

        return self._aggregate(Statistic.MEAN, override_options)

    def reset_options(self) -> None:
        """
        Method to reset/clear current table options.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        self.options.property_filter = None
        self.options.columns = None
        self.options.aoi = None

    def delete(self) -> None:
        """
        Delete this Vector product.

        This function will disable all subsequent non-static method calls.
        """
        products_delete(self.id)


class FeatureCollection(gpd.GeoDataFrame):
    """
    A class for interacting with Vector features.
    """

    def __init__(self, id: str, *args, **kwargs):
        """
        Initialize a FeatureCollection instance.

        Users should create a Feature instance via `FeatureSearch.collect`.

        Parameters
        ----------
        id: str
            The feature id.
        """
        super().__init__(*args, **kwargs)

        self._id = id

    @property
    def id(self):
        """
        Return the product id of the Table.

        Returns
        -------
        product_id: str
            Table product ID
        """
        return self._id

    @property
    def table(self):
        """
        Return the Table of the FeatureCollection.

        Returns
        -------
        table: Table
            Table
        """
        return Table.get(self._id)


class Feature:
    """
    A class for interacting with a Vector feature.
    """

    def __init__(self, id: str, df: gpd.GeoDataFrame):
        """
        Initialize a Feature instance.

        Users should create a Feature instance via `Table.get_feature`.

        Parameters
        ----------
        id: str
            The feature id.
        """
        self._id = id
        self._values = {}
        for k, v in df.to_dict().items():
            self._values[k] = v[0]

    @property
    def values(self):
        """
        Return the id of the Vector Feature.

        Returns
        -------
        id: str
            Feature ID
        """
        return self._values

    @values.setter
    def values(self, key, value):
        """
        Return the id of the Vector Feature.

        Returns
        -------
        id: str
            Feature ID
        """
        self._values[key] = value

    @property
    def id(self):
        """
        Return the id of the Vector Feature.

        Returns
        -------
        id: str
            Feature ID
        """
        return self._id

    @property
    def product_id(self):
        """
        Return the product id of the Table.

        Returns
        -------
        product_id: str
            Table product ID
        """
        return ":".join(self._id.split(":")[:-1])

    @property
    def name(self):
        """
        Return the name/uuid of the Vector Feature.

        Returns
        -------
        name: str
            Feature name
        """
        return self._id.split(":")[-1]

    @property
    def table(self):
        """
        Return the Table of the FeatureCollection.

        Returns
        -------
        table: Table
            Table
        """
        return Table.get(self.product_id)

    @staticmethod
    def get(id: str) -> Feature:
        """
        Get a Feature instance associated with a Vector Feature ID.

        Parameters
        ----------
        id: str
            ID of Feature

        Returns
        -------
        feature: Feature
            Feature instance for the feature ID.
        """
        pid = ":".join(id.split(":")[0:-1])
        fid = id.split(":")[-1]

        df = features_get(pid, fid)

        return Feature(id, df)

    def save(self) -> None:
        """
        Save/update this Vector Feature.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        gdf = gpd.GeoDataFrame.from_features([self], crs="EPSG:4326")
        features_update(self.product_id, self.name, gdf)

    def delete(self) -> None:
        """
        Delete this Vector Feature.

        Parameters
        ----------
        None

        Returns
        -------
        None
        """
        features_delete(self.product_id, self.name)

    @property
    def __geo_interface__(self):
        return {
            "geometry": self.values["geometry"].__geo_interface__,
            "properties": {
                c: self.values[c] for c in self.table.columns if c != "geometry"
            },
        }
