from click.testing import CliRunner

from src.scripts.readactor import cli


def test_cli():
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert result.output == "version 1.0.1a0"
