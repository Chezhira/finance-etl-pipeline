import re
from typer.testing import CliRunner
from finance_etl.cli import app

# Regex to remove ANSI escape sequences (e.g., \x1b[1m)
ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

runner = CliRunner()

def _clean(text: str) -> str:
    return ANSI_RE.sub("", text)

def test_run_help_has_options():
    result = runner.invoke(app, ["run", "--help"], color=False)
    assert result.exit_code == 0
    out = _clean(result.stdout)
    assert "--month" in out
    assert "--fail-on" in out

def test_version_command():
    result = runner.invoke(app, ["version"], color=False)
    assert result.exit_code == 0
    out = _clean(result.stdout)
    assert "finance-etl" in out
