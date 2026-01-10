from typer.testing import CliRunner
from finance_etl.cli import app

runner = CliRunner(mix_stderr=False)

def test_run_help_has_options():
    result = runner.invoke(app, ["run", "--help"], color=False)
    assert result.exit_code == 0
    out = result.stdout
    assert "--month" in out
    assert "--fail-on" in out

def test_version_command():
    result = runner.invoke(app, ["version"], color=False)
    assert result.exit_code == 0
    assert "finance-etl" in result.stdout
