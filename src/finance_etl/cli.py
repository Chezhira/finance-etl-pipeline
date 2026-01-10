
from typer import Typer, Option

# Force a multi-command app (group) and show help when no args are given
app = Typer(help="Finance ETL CLI", no_args_is_help=True, add_completion=False)

@app.command("run")
def run_cmd(
    month: str = Option(..., "--month", "-m", help="Target month, e.g., 2025-12"),
    fail_on: str = Option("ERROR", "--fail-on", help="DQ strictness: ERROR|WARN|NEVER"),
):
    """
    Run the monthly-close pipeline.

    Example:
      finance-etl run --month 2025-12 --fail-on WARN
    """
    # TODO: wire up your real pipeline here
    # from finance_etl.pipeline import run_pipeline
    # run_pipeline(month=month, fail_on=fail_on)
    print(f"Running ETL for month={month}, fail_on={fail_on}")

@app.command("version")
def version_cmd():
    """Show CLI version (placeholder to keep multi-command layout)."""
    try:
        # If you set version in pyproject, you can read it dynamically:
        from importlib.metadata import version
        print("finance-etl", version("finance-etl"))
    except Exception:
        print("finance-etl 0.1.0")

if __name__ == "__main__":
    # Running as a module or script should now show a COMMANDS section.
    app()
