"""Expression utility library"""

import dataclasses
from collections.abc import Callable
from typing import Any, Dict, List, Union

import descarteslabs.core.common.property_filtering.filtering as dl_filt


def make_parse_error(msg: str, path: str) -> ValueError:
    """Constructor"""
    if path == "":
        path = "<root>"
    err_msg = (
        f'Parse Error: Descartes Labs filtering expression at path "{path}":\n'
        f"    {msg}"
    )
    return ValueError(err_msg)


def json_parse_expression(
    data: Dict[str, Any]
) -> Union[dl_filt.Expression, dl_filt.AndExpression, dl_filt.OrExpression]:
    """Parses a DL filtering expression from its JSON encoding"""
    return _json_parse_expression(data, path="")


def _json_parse_expression(
    data: Dict[str, Any], *, path: str
) -> Union[dl_filt.Expression, dl_filt.AndExpression, dl_filt.OrExpression]:
    """Parses a DL filtering expression from its JSON encoding"""
    _validate_dtype(data, dict, path=path)
    _validate_len_exact(data, 1, path=path)

    operation, val = next(iter(data.items()))

    path += "/"

    schema = SCHEMA.get(operation)
    if schema is None:
        msg = f'Unknown expression operation: "{operation}"'
        raise make_parse_error(msg, path)

    path += f"{operation}"

    if not isinstance(val, schema.value_dtype):
        msg = (
            f'Excpected value of type "{schema.value_dtype.__name__}", '
            f'got "{type(val).__name__}"'
        )
        raise make_parse_error(msg, path)

    return schema.callback(val, path=path)


def _validate_dtype(val: Any, dtype: type, *, path: str) -> None:
    """Validates the data type of the given value"""
    if not isinstance(val, dtype):
        msg = f"Value must be a {dtype.__name__} (was {type(val).__name__})"
        raise make_parse_error(msg, path)


def _validate_len_exact(val: Any, exact_len: int, *, path: str) -> None:
    """Validates the exact length of the given value"""
    if len(val) != exact_len:
        entries = "entries" if exact_len != 1 else "entry"
        msg = f"Value must have exactly {exact_len} {entries} (had {len(val)})"
        raise make_parse_error(msg, path)


def _validate_len_ge(val: Any, ge_len: int, *, path: str) -> None:
    """Validates the exact length of the given value"""
    if len(val) < ge_len:
        entries = "entries" if ge_len != 1 else "entry"
        msg = f"Value must have at least {ge_len} {entries} (had {len(val)})"
        raise make_parse_error(msg, path)


def _parse_and_expression(
    data: List[Dict[str, Any]], *, path: str
) -> dl_filt.AndExpression:
    """Makes an AndExpression from its JSON serialization"""
    _validate_len_ge(data, 2, path=path)

    parts = []
    for idx, part in enumerate(data):
        parts.append(_json_parse_expression(part, path=f"{path}[{idx}]"))
    return dl_filt.AndExpression(parts)


def _parse_or_expression(
    data: List[Dict[str, Any]], *, path: str
) -> dl_filt.OrExpression:
    """Makes an OrExpression from its JSON serialization"""
    _validate_len_ge(data, 2, path=path)

    parts = []
    for idx, part in enumerate(data):
        parts.append(_json_parse_expression(part, path=f"{path}[{idx}]"))
    return dl_filt.OrExpression(parts)


def _parse_eq_expression(data: Dict[str, Any], *, path: str) -> dl_filt.EqExpression:
    """Makes an EqExpression from its JSON serialization"""
    _validate_len_exact(data, 1, path=path)
    field, val = next(iter(data.items()))
    return dl_filt.EqExpression(field, val)


def _parse_ne_expression(data: Dict[str, Any], *, path: str) -> dl_filt.NeExpression:
    """Makes an NeExpression from its JSON serialization"""
    _validate_len_exact(data, 1, path=path)
    field, val = next(iter(data.items()))
    return dl_filt.NeExpression(field, val)


def _parse_range_expression(
    data: Dict[str, Any], *, path: str
) -> dl_filt.RangeExpression:
    """Makes a RangeExpression from its JSON serialization"""
    _validate_len_exact(data, 1, path=path)

    field, exprs = next(iter(data.items()))

    path += f"/{field}"
    _validate_dtype(exprs, dict, path=path)
    _validate_len_ge(exprs, 1, path=path)
    for key in exprs.keys():
        if key not in ("gte", "gt", "lte", "lt"):
            msg = f'Unknown operation for range expression: "{key}"'
            raise make_parse_error(msg, path)

    return dl_filt.RangeExpression(field, exprs)


def _parse_is_null_expression(data: str, *, path: str) -> dl_filt.IsNullExpression:
    """Makes an IsNullExpression from its JSON serialization"""
    del path
    return dl_filt.IsNullExpression(data)


def _parse_is_not_null_expression(
    data: str, *, path: str
) -> dl_filt.IsNotNullExpression:
    """Makes an IsNotNullExpression from its JSON serialization"""
    del path
    return dl_filt.IsNotNullExpression(data)


def _parse_prefix_expression(
    data: Dict[str, Any], *, path: str
) -> dl_filt.PrefixExpression:
    """Makes a PrefixExpression from its JSON serialization"""
    _validate_len_exact(data, 1, path=path)
    field, val = next(iter(data.items()))
    return dl_filt.PrefixExpression(field, val)


def _parse_like_expression(
    data: Dict[str, Any], *, path: str
) -> dl_filt.LikeExpression:
    """Makes a LikeExpression from its JSON serialization"""
    _validate_len_exact(data, 1, path=path)
    field, val = next(iter(data.items()))
    return dl_filt.LikeExpression(field, val)


@dataclasses.dataclass
class _SchemaEntry:
    """Entry in the schema"""

    value_dtype: type
    callback: Callable


SCHEMA = {
    "and": _SchemaEntry(
        value_dtype=List,
        callback=_parse_and_expression,
    ),
    "or": _SchemaEntry(
        value_dtype=List,
        callback=_parse_or_expression,
    ),
    "eq": _SchemaEntry(
        value_dtype=Dict,
        callback=_parse_eq_expression,
    ),
    "ne": _SchemaEntry(
        value_dtype=Dict,
        callback=_parse_ne_expression,
    ),
    "range": _SchemaEntry(
        value_dtype=Dict,
        callback=_parse_range_expression,
    ),
    "isnull": _SchemaEntry(
        value_dtype=str,
        callback=_parse_is_null_expression,
    ),
    "isnotnull": _SchemaEntry(
        value_dtype=str,
        callback=_parse_is_not_null_expression,
    ),
    "prefix": _SchemaEntry(
        value_dtype=dict,
        callback=_parse_prefix_expression,
    ),
    "like": _SchemaEntry(
        value_dtype=dict,
        callback=_parse_like_expression,
    ),
}
