from typing import Any, Dict
from uuid import uuid4

from pydantic import BaseModel, Field


class VectorBaseModel(BaseModel):
    uuid: str = Field(
        default_factory=uuid4,
        json_schema_extra={"primary_key": True},
    )
    product_id: str


class GeometryBaseModel(VectorBaseModel):
    geometry: str = Field(json_schema_extra={"geometry": "GEOMETRY"})


class PointBaseModel(VectorBaseModel):
    geometry: str = Field(json_schema_extra={"geometry": "POINT"})


class LineBaseModel(VectorBaseModel):
    geometry: str = Field(json_schema_extra={"geometry": "LINE"})


class PolygonBaseModel(VectorBaseModel):
    geometry: str = Field(json_schema_extra={"geometry": "POLYGON"})


class MultiPointBaseModel(VectorBaseModel):
    geometry: str = Field(json_schema_extra={"geometry": "MULTIPOINT"})


class MultiLineBaseModel(VectorBaseModel):
    geometry: str = Field(json_schema_extra={"geometry": "MULTILINE"})


class MultiPolygonBaseModel(VectorBaseModel):
    geometry: str = Field(json_schema_extra={"geometry": "MULTIPOLYGON"})


class GenericFeatureBaseModel(GeometryBaseModel):
    properties: Dict[str, Any] = {}
