"""
Command-Line Interface for SAP to PySpark Framework
Built with Rich for high-visibility terminal output.
"""

import os
import sys
import argparse
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.syntax import Syntax

from sap_to_pyspark.engine.registry import TCodeRegistry
from sap_to_pyspark.engine.generator import PySparkCodeGenerator
from sap_to_pyspark.engine.abap_parser import ABAPQueryParser
from sap_to_pyspark.engine.databricks import DatabricksJobBuilder
from sap_to_pyspark.simulator.runner import SAPPipelineRunner
from sap_to_pyspark.metadata.data_dictionary import SAPDataDictionary

console = Console()


def cmd_list(args):
    """Lists all pre-configured SAP T-Codes."""
    table = Table(title="Pre-configured SAP T-Codes to PySpark Catalog", show_header=True, header_style="bold magenta")
    table.add_column("T-Code", style="cyan", width=10)
    table.add_column("Module", style="green", width=8)
    table.add_column("Title", style="white", width=30)
    table.add_column("Base Table", style="yellow", width=12)
    table.add_column("Target Lakehouse Table", style="blue")

    all_specs = TCodeRegistry.get_all()
    for code, spec in all_specs.items():
        table.add_row(spec.tcode, spec.module, spec.title, spec.base_table, spec.target_table_name)

    console.print(table)
    console.print(f"\n[bold green]Total available T-Codes: {len(all_specs)}[/bold green]")


def cmd_info(args):
    """Displays detailed architecture metadata for a specific T-Code."""
    tcode = args.tcode.upper()
    spec = TCodeRegistry.get(tcode)
    if not spec:
        console.print(f"[bold red]Error:[/bold red] T-Code '{tcode}' not found in registry.")
        sys.exit(1)

    panel_text = (
        f"[bold]T-Code:[/bold] {spec.tcode} ({spec.title})\n"
        f"[bold]Module:[/bold] {spec.module}\n"
        f"[bold]Description:[/bold] {spec.description}\n"
        f"[bold]Base Table:[/bold] {spec.base_table}\n"
        f"[bold]Target Table:[/bold] {spec.target_layer}.{spec.target_table_name} ({spec.target_format})\n"
        f"[bold]Partitioning:[/bold] {spec.partition_by or 'None'}"
    )
    console.print(Panel(panel_text, title=f"SAP T-Code Metadata: {spec.tcode}", border_style="cyan"))

    # Joins Table
    if spec.joins:
        j_tbl = Table(title="Joins", show_header=True, header_style="bold blue")
        j_tbl.add_column("Left Table")
        j_tbl.add_column("Right Table")
        j_tbl.add_column("Type")
        j_tbl.add_column("Join Keys")
        j_tbl.add_column("Broadcast Hint")
        for j in spec.joins:
            j_tbl.add_row(j.left_table, j.right_table, j.join_type, str(j.join_keys), "YES" if j.is_broadcast else "NO")
        console.print(j_tbl)

    # Fields Table
    f_tbl = Table(title="Output Fields & Transformations", show_header=True, header_style="bold green")
    f_tbl.add_column("Source Table")
    f_tbl.add_column("Field")
    f_tbl.add_column("PySpark Alias")
    f_tbl.add_column("Cast Type")
    f_tbl.add_column("Rule / Formula")
    for f in spec.fields:
        rule_desc = f.transform_rule or (f.custom_expr if f.custom_expr else "-")
        f_tbl.add_row(f.table or spec.base_table, f.field, f.alias or "-", f.cast_type or "-", rule_desc)
    console.print(f_tbl)


def cmd_generate(args):
    """Generates PySpark ETL code and test file for a T-Code."""
    tcode = args.tcode.upper()
    spec = TCodeRegistry.get(tcode)
    if not spec:
        console.print(f"[bold red]Error:[/bold red] T-Code '{tcode}' not found in registry.")
        sys.exit(1)

    gen = PySparkCodeGenerator()
    code = gen.generate_pipeline_code(spec)
    test_code = gen.generate_unit_test_code(spec)

    if args.output:
        os.makedirs(os.path.dirname(os.path.abspath(args.output)) or ".", exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(code)
        console.print(f"[bold green][SUCCESS] PySpark pipeline written to:[/bold green] {args.output}")
    else:
        syntax = Syntax(code, "python", theme="monokai", line_numbers=True)
        console.print(Panel(syntax, title=f"Generated PySpark Pipeline: {spec.tcode}", border_style="green"))

    if args.test_output:
        os.makedirs(os.path.dirname(os.path.abspath(args.test_output)) or ".", exist_ok=True)
        with open(args.test_output, "w", encoding="utf-8") as f:
            f.write(test_code)
        console.print(f"[bold green][SUCCESS] PySpark unit test suite written to:[/bold green] {args.test_output}")


def cmd_parse_abap(args):
    """Parses raw ABAP OpenSQL query and generates PySpark script."""
    query = args.query
    if not query and args.file:
        with open(args.file, "r", encoding="utf-8") as f:
            query = f.read()

    if not query:
        console.print("[bold red]Error:[/bold red] Provide either --query or --file with ABAP OpenSQL.")
        sys.exit(1)

    console.print("[bold yellow]Parsing ABAP OpenSQL query...[/bold yellow]")
    parser = ABAPQueryParser()
    spec = parser.parse(query, tcode=args.tcode or "Z_REPORT", target_table_name=args.target_table or "custom_gold_table")

    gen = PySparkCodeGenerator()
    code = gen.generate_pipeline_code(spec)

    if args.output:
        os.makedirs(os.path.dirname(os.path.abspath(args.output)) or ".", exist_ok=True)
        with open(args.output, "w", encoding="utf-8") as f:
            f.write(code)
        console.print(f"[bold green][SUCCESS] Translated PySpark code written to:[/bold green] {args.output}")
    else:
        syntax = Syntax(code, "python", theme="monokai", line_numbers=True)
        console.print(Panel(syntax, title="Translated PySpark Pipeline from ABAP", border_style="magenta"))


def cmd_test(args):
    """Runs transformation pipeline against synthetic SAP mock data."""
    tcode = args.tcode.upper()
    spec = TCodeRegistry.get(tcode)
    if not spec:
        console.print(f"[bold red]Error:[/bold red] T-Code '{tcode}' not found in registry.")
        sys.exit(1)

    console.print(f"[bold cyan]Running simulated ETL pipeline for {spec.tcode}...[/bold cyan]")
    runner = SAPPipelineRunner()
    result = runner.run_spec(spec)

    if not result["success"]:
        console.print(f"[bold red]Simulation Failed:[/bold red] {result.get('error')}")
        sys.exit(1)

    console.print(f"[bold green][SUCCESS] Simulation Succeeded in {result['duration_ms']}ms![/bold green]")
    console.print(f"Total Output Rows: [bold cyan]{result['row_count']}[/bold cyan]")

    for diag in result["diagnostics"]:
        console.print(f"  * {diag}")

    # Render Sample Data Table
    if result["sample_data"]:
        sample_tbl = Table(title=f"Sample Transformed Data ({spec.tcode})", show_header=True, header_style="bold cyan")
        columns = result["columns"][:8]  # Show first 8 columns for terminal width
        for c in columns:
            sample_tbl.add_column(c)

        for row in result["sample_data"][:5]:
            sample_tbl.add_row(*[str(row.get(c, "")) for c in columns])

        console.print(sample_tbl)


def cmd_export_databricks(args):
    """Exports a Databricks Serverless Workflow Job JSON and DABs YAML bundle."""
    import json
    tcode = args.tcode.upper()
    spec = TCodeRegistry.get(tcode)
    if not spec:
        console.print(f"[bold red]Error:[/bold red] T-Code '{tcode}' not found in registry.")
        sys.exit(1)

    catalog = args.catalog or "main"
    schema = args.schema or "sap_lakehouse"
    script_path = args.script_path or f"/Workspace/Shared/sap_pipelines/{spec.tcode.lower()}_pipeline.py"

    job_spec = DatabricksJobBuilder.generate_serverless_job_json(
        spec=spec,
        workspace_script_path=script_path,
        unity_catalog=catalog,
        unity_schema=schema,
    )
    dab_yaml = DatabricksJobBuilder.generate_dab_bundle_yaml(
        spec=spec,
        unity_catalog=catalog,
        unity_schema=schema,
    )

    if args.output_job:
        os.makedirs(os.path.dirname(os.path.abspath(args.output_job)) or ".", exist_ok=True)
        with open(args.output_job, "w", encoding="utf-8") as f:
            json.dump(job_spec, f, indent=2)
        console.print(f"[bold green][SUCCESS] Serverless Databricks Job JSON saved to:[/bold green] {args.output_job}")
    else:
        syntax_json = Syntax(json.dumps(job_spec, indent=2), "json", theme="monokai", line_numbers=True)
        console.print(Panel(syntax_json, title=f"Databricks Serverless Job Definition: {spec.tcode}", border_style="cyan"))

    if args.output_dab:
        os.makedirs(os.path.dirname(os.path.abspath(args.output_dab)) or ".", exist_ok=True)
        with open(args.output_dab, "w", encoding="utf-8") as f:
            f.write(dab_yaml)
        console.print(f"[bold green][SUCCESS] Databricks Asset Bundle (DAB) YAML saved to:[/bold green] {args.output_dab}")


def main():
    parser = argparse.ArgumentParser(description="SAP T-Code to PySpark Conversion CLI")
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # list
    subparsers.add_parser("list", help="List all registered SAP T-Codes")

    # info
    p_info = subparsers.add_parser("info", help="Show metadata and join architecture for a T-Code")
    p_info.add_argument("--tcode", required=True, help="SAP T-Code (e.g. VA05, FBL3N)")

    # generate
    p_gen = subparsers.add_parser("generate", help="Generate PySpark code for a T-Code")
    p_gen.add_argument("--tcode", required=True, help="SAP T-Code (e.g. VA05, FBL3N)")
    p_gen.add_argument("--output", "-o", help="File path to save generated PySpark pipeline (.py)")
    p_gen.add_argument("--test-output", help="File path to save generated PySpark unit tests (.py)")

    # parse-abap
    p_abap = subparsers.add_parser("parse-abap", help="Parse ABAP OpenSQL query into PySpark pipeline")
    p_abap.add_argument("--query", "-q", help="ABAP SELECT statement string")
    p_abap.add_argument("--file", "-f", help="File containing ABAP SQL statement")
    p_abap.add_argument("--tcode", default="Z_REPORT", help="Custom T-Code name")
    p_abap.add_argument("--target-table", default="custom_gold_table", help="Target table name")
    p_abap.add_argument("--output", "-o", help="File path to save generated PySpark pipeline (.py)")

    # test
    p_test = subparsers.add_parser("test", help="Test transformation on mock SAP Bronze data")
    p_test.add_argument("--tcode", required=True, help="SAP T-Code (e.g. VA05, FBL3N)")

    # export-databricks
    p_dbx = subparsers.add_parser("export-databricks", help="Export Databricks Serverless Job and DABs bundle")
    p_dbx.add_argument("--tcode", required=True, help="SAP T-Code (e.g. VA05, FBL3N)")
    p_dbx.add_argument("--catalog", default="main", help="Target Unity Catalog name")
    p_dbx.add_argument("--schema", default="sap_lakehouse", help="Target Unity Catalog schema name")
    p_dbx.add_argument("--script-path", help="Workspace path to python script")
    p_dbx.add_argument("--output-job", help="File path to save Databricks Job JSON")
    p_dbx.add_argument("--output-dab", help="File path to save DABs databricks.yml")

    args = parser.parse_args()
    if not args.command:
        parser.print_help()
        sys.exit(0)

    dispatch = {
        "list": cmd_list,
        "info": cmd_info,
        "generate": cmd_generate,
        "parse-abap": cmd_parse_abap,
        "test": cmd_test,
        "export-databricks": cmd_export_databricks,
    }

    func = dispatch.get(args.command)
    if func:
        func(args)


if __name__ == "__main__":
    main()
