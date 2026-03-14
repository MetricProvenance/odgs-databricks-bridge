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
def write_back(
    log_path: str = typer.Option(
        "./sovereign_audit.log", "--log-path", "-l", help="Path to the ODGS audit log file"
    ),
    url: str = typer.Option(
        ..., "--url", "-u", help="Databricks workspace URL",
        envvar="DATABRICKS_HOST",
    ),
    token: str = typer.Option(
        None, "--token", "-t", help="Personal access token",
        envvar="DATABRICKS_TOKEN",
    ),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Enable debug logging"),
):
    """
    Asynchronous Plane 4 Write-Back: 
    Parses the local ODGS audit log and pushes validation URLs back to Databricks tables.
    """
    import json
    from odgs_databricks.client import UnityCatalogClient
    
    logging.basicConfig(
        level=logging.DEBUG if verbose else logging.INFO,
        format="%(levelname)s  %(message)s",
    )

    if not token:
        typer.echo("Error: Provide --token or set DATABRICKS_TOKEN.", err=True)
        raise typer.Exit(1)

    if not os.path.exists(log_path):
        typer.echo(f"Error: Audit log not found at {log_path}", err=True)
        raise typer.Exit(1)

    client = UnityCatalogClient(workspace_url=url, token=token)

    processed_count = 0
    with open(log_path, "r", encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            
            try:
                json_str = line.split(" - ", 1)[1] if " - " in line else line
                entry = json.loads(json_str)
            except Exception as e:
                logging.debug(f"Skipping unparseable line: {e}")
                continue

            metadata_map = entry.get("applied_metadata", {})
            outcome = entry.get("execution_result", "UNKNOWN")
            payload_hash = entry.get("tri_partite_binding", {}).get("payload_hash", "")
            
            # Find Databricks tables
            dbx_tables = set()
            for rule_id, meta in metadata_map.items():
                if isinstance(meta, dict) and "databricks_table" in meta:
                    dbx_tables.add(meta["databricks_table"])

            for table_name in dbx_tables:
                cert_url = f"https://certificate.metricprovenance.com/?hash={payload_hash}"
                emoji = "🛑" if outcome == "BLOCKED" else "✅"
                
                # We overwrite or append the comment. Unity catalog handles markdown in comments.
                comment_text = (
                    f"**ODGS Enforcement: {outcome}** {emoji}\n\n"
                    f"A data pipeline governance check was executed against this table.\n"
                    f"**Forensic Hash:** `{payload_hash}`\n"
                    f"**Validation & Certificate:** [View S-Cert or Upgrade to Sovereign Pack]({cert_url})"
                )
                
                try:
                    client.update_table_comment(table_name, comment_text)
                    logging.info(f"Successfully wrote back to Databricks Table {table_name}")
                    processed_count += 1
                except Exception as e:
                    logging.error(f"Failed to write to Databricks Table {table_name}: {e}")

    typer.echo(f"\n✅ Bi-Directional Sync Complete. Wrote {processed_count} logs to Databricks.")

@app.command()
def version():
    """Show the bridge version."""
    from odgs_databricks import __version__
    typer.echo(f"odgs-databricks-bridge v{__version__}")


if __name__ == "__main__":
    app()
