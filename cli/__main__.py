"""Command-line interface to DL Vector service"""

import json
import sys
from typing import Any, Dict, Iterable, List, Optional, TextIO

import click
import descarteslabs.auth as dl_auth
import descarteslabs.vector as dlv
import geojson
import toml

from . import expressions
from .sharing import Role, share_product, unshare_product


def table_repr(table: dlv.Table) -> Dict[str, Any]:
    """Gets a dictionary representation of a table suitable for printing"""
    return json.loads(repr(table))


# The inconsistent path terminates the program
# pylint: disable=inconsistent-return-statements
def get_table_or_fail(product_id: str) -> dlv.Table:
    """Gets the given table, exit with a failure if it doesn't exist"""
    try:
        return dlv.Table.get(product_id)
    except (
        dlv.vector_exceptions.RedirectException,
        dlv.vector_exceptions.ClientException,
        dlv.vector_exceptions.ServerException,
        dlv.vector_exceptions.GenericException,
    ) as e:
        log_fatal(str(e))


def log_fatal(msg: str) -> None:
    """Log an error message and exit"""
    click.echo(msg, err=True)
    sys.exit(1)


@click.group()
def cli() -> None:
    """DL Catalog CLI"""
    # try to auth
    dl_auth.Auth()


@cli.command("create-table")
@click.argument(
    "config_file",
    type=click.File("r"),
)
def create_table(config_file: TextIO) -> None:
    """Creates a new DL Vector product

    Parameters
    ----------
    config_file : TextIO
        The config file used for creating the product
    """
    # verify args
    data = toml.load(config_file)
    click.confirm(
        f"Creating product: '{data.get('product_id')}', do you wish to continue?",
        abort=True,
    )

    # only required field is product_id
    table = dlv.Table.create(
        data.get("product_id"),
        name=data.get("name"),
        description=data.get("description"),
        tags=data.get("tags"),
    )
    click.echo(json.dumps(table_repr(table), indent=2))


@cli.command("delete-table")
@click.argument("product_id")
@click.option(
    "-y", "--auto-confirm", is_flag=True, help="Optional flag to bypass the prompt."
)
def delete_table(product_id: str, auto_confirm: bool) -> None:
    """Deletes the specified table"""
    table = get_table_or_fail(product_id)
    if not auto_confirm:
        click.confirm(
            f"Deleting product: '{product_id}', do you wish to continue?",
            abort=True,
        )
    table.delete()
    click.echo("Deleted.")


@cli.command("list-tables")
@click.option(
    "-t",
    "--tag",
    multiple=True,
    type=str,
    help="Optional tag(s) a table must have to be returned",
)
def list_tables(*, tag: Iterable[str]) -> None:
    """Lists the available vector products"""
    products = dlv.Table.list(list(tag))
    click.echo(json.dumps([table_repr(p) for p in products], indent=2))


@cli.command("describe-table")
@click.argument("product_id")
def describe_table(product_id: str) -> None:
    """Describes the table with the given ID"""
    table = get_table_or_fail(product_id)
    click.echo(json.dumps(table_repr(table), indent=2))


@cli.command("ingest")
@click.argument("product_id")
@click.argument("geojson_file", type=click.File("r"))
@click.option(
    "-y", "--auto-confirm", is_flag=True, help="Optional flag to bypass the prompt."
)
def ingest(product_id: str, geojson_file: TextIO, auto_confirm: bool) -> None:
    """Ingests a FeatureCollection from the given file"""

    table = get_table_or_fail(product_id)

    try:
        with geojson_file:
            input_gj = geojson.load(geojson_file)
    except json.JSONDecodeError as e:
        log_fatal(f"JSON error in input GeoJSON:\n{str(e)}")

    if not input_gj.get("features"):
        log_fatal(
            "Missing field 'features'. Input data set must be a FeatureCollection."
        )
    if not auto_confirm:
        click.confirm(
            f'Adding {len(input_gj.features)} features to "{product_id}", do you '
            "wish to continue?",
            abort=True,
        )
    result = table.add(input_gj)
    uuid_list = []
    for feature in result.feature_collection["features"]:
        uuid_list.append(feature["uuid"])
    click.echo(f"{uuid_list}")


@cli.command("list-features")
@click.option(
    "-f",
    "--search-json-file",
    type=click.File("r"),
    help="File containing search JSON specification",
)
@click.option(
    "-j", "--search-json", type=str, help="Search JSON specification (overrides file)"
)
@click.argument("product_id")
def list_features(
    product_id: str,
    *,
    search_json_file: Optional[TextIO] = None,
    search_json: Optional[str] = None,
) -> None:
    """Lists the features in the given product"""
    if search_json is None and search_json_file is not None:
        with search_json_file:
            search_json = json.load(search_json_file)

    aoi = None
    search_filters = None
    if search_json is not None:
        aoi = search_json.get("aoi")
        search_filters = search_json.get("filter")
        try:
            aoi = geojson.loads(aoi) if aoi is not None else None
            search_filters = (
                expressions.json_parse_expression(search_filters)
                if search_filters is not None
                else None
            )
        except json.JSONDecodeError as e:
            log_fatal(f"JSON error in input GeoJSON:\n{str(e)}")

    table = get_table_or_fail(product_id)
    feats = table.features(aoi=aoi, filter=search_filters).collect()
    click.echo(geojson.dumps(feats.feature_collection, indent=2))


@cli.command("describe-feature")
@click.argument("product_id")
@click.argument("feature_id")
def describe_feature(product_id: str, feature_id: str) -> None:
    """Describes the feature in the given product with the given ID"""
    table = get_table_or_fail(product_id)
    try:
        click.echo(json.dumps(table.get_feature(feature_id), indent=2))
    except Exception as e:  # pylint: disable=broad-except
        log_fatal(str(e))


@cli.command("delete-feature")
@click.argument("product_id")
@click.argument("feature_id")
@click.option(
    "-y", "--auto-confirm", is_flag=True, help="Optional flag to bypass the prompt."
)
def delete_feature(product_id: str, feature_id: str, auto_confirm: bool) -> None:
    """Deletes the feature with the given ID from the given product"""
    table = get_table_or_fail(product_id)
    if not auto_confirm:
        click.confirm(
            f'Deleting feature "{feature_id}" from "{product_id}", '
            "do you wish to continue?",
            abort=True,
        )
    table.delete_feature(feature_id)


def common_share_unshare_options(function):
    """Common click options for share_product and unshare_product"""

    function = click.argument("product_id")(function)
    function = click.option(
        "--role",
        required=True,
        type=click.Choice(Role.values(), case_sensitive=True),
    )(function)
    function = click.option(
        "--org", multiple=True, help="Orgnization(s) with which to share"
    )(function)
    function = click.option(
        "--user", multiple=True, help="User ID(s) with which to share"
    )(function)
    function = click.option(
        "--group", multiple=True, help="Group(s) with which to share"
    )(function)
    function = click.option(
        "--email", multiple=True, help="Email(s) with which to share"
    )(function)
    return function


@cli.command("share-table")
@common_share_unshare_options
def share_table(
    product_id: str,
    *,
    role: str,
    org: List[str],
    user: List[str],
    group: List[str],
    email: List[str],
) -> None:
    """Shares the given product with the given entities"""
    table = get_table_or_fail(product_id)

    share_product(
        table,
        role=Role(role),
        orgs=org,
        users=user,
        groups=group,
        emails=email,
    )


@cli.command("unshare-table")
@common_share_unshare_options
def unshare_table(
    product_id: str,
    *,
    role: str,
    org: List[str],
    user: List[str],
    group: List[str],
    email: List[str],
) -> None:
    """Unshares the given product with the given entities"""
    try:
        table = get_table_or_fail(product_id)

        result = unshare_product(
            table,
            role=Role(role),
            orgs=org,
            users=user,
            groups=group,
            emails=email,
        )
    except (
        dlv.vector_exceptions.RedirectException,
        dlv.vector_exceptions.ClientException,
        dlv.vector_exceptions.ServerException,
        dlv.vector_exceptions.GenericException,
    ) as e:
        click.echo(
            f"Attempted to unshare product {product_id}.\n Error: {e}.", err=True
        )
    for unknown in result.unknown_principals:
        click.echo(f'"{unknown}" was not a {role} on "{product_id}"')


if __name__ == "__main__":
    cli()  # pylint: disable=no-value-for-parameter missing-kwoa
