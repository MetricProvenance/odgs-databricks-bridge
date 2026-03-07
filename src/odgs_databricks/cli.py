"""
ODGS Databricks Bridge — CLI
==============================

Command-line interface for syncing Databricks Unity Catalog
metadata into ODGS enforcement schemas.

Usage:
    odgs-databricks sync \
        --url https://adb-1234.azuredatabricks.net \
        --token dapi... \
        --org acme_corp \
        --catalog production
"""

import os
import sys
import logging

try:
    import typer
except ImportError:
    print("Error: typer is required. Install with: pip install typer>=0.9.0")
    sys.exit(1)

from odgs_databricks.bridge import DatabricksBridge

app = typer.Typer(
    name="odgs-databricks",
    help="Bridge: Databricks Unity Catalog → ODGS Runtime Enforcement Schemas",
    no_args_is_help=True,
)


@app.command()
def sync(
    url: str = typer.Option(
        ..., "--url", "-u", help="Databricks workspace URL",
        envvar="DATABRICKS_HOST",
    ),
    token: str = typer.Option(
        None, "--token", "-t", help="Personal access token",
        envvar="DATABRICKS_TOKEN",
    ),
    org: str = typer.Option(
        ..., "--org", "-o", help="Organization name for URN namespace",
    ),
    catalog: str = typer.Option(
        ..., "--catalog", "-c", help="Unity Catalog name to scan",
    ),
    schema_filter: str = typer.Option(
        None, "--schema", "-s", help="Filter to specific schema",
    ),
    output: str = typer.Option(
        "./schemas/custom/", "--output", help="Output directory for ODGS schemas",
    ),
    output_type: str = typer.Option(
        "metrics", "--type", help="Output type: 'metrics' or 'rules'",
    ),
    severity: str = typer.Option(
        "WARNING", "--severity", help="Rule severity (HARD_STOP, WARNING, INFO)",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
):
    """Sync Unity Catalog tables into ODGS JSON schemas."""
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s  %(message)s",
    )

    if not token:
        typer.echo(
            "Error: Provide --token or set DATABRICKS_TOKEN environment variable.",
            err=True,
        )
        raise typer.Exit(1)

    try:
        bridge = DatabricksBridge(
            workspace_url=url,
            token=token,
            organization=org,
        )
        filepath = bridge.sync(
            catalog=catalog,
            schema_filter=schema_filter,
            output_dir=output,
            output_type=output_type,
            severity=severity,
        )

        if filepath:
            typer.echo(f"\n✅ ODGS schema written to: {filepath}")
        else:
            typer.echo("\n⚠️  No tables found matching the given filters.", err=True)
            raise typer.Exit(1)

    except Exception as e:
        typer.echo(f"\n❌ Bridge Error: {e}", err=True)
        raise typer.Exit(1)


@app.command()
def version():
    """Show the bridge version."""
    from odgs_databricks import __version__
    typer.echo(f"odgs-databricks-bridge v{__version__}")


if __name__ == "__main__":
    app()
