from click.testing import CliRunner

from src.scripts.readactor import cli


def test_version1():
    runner = CliRunner()
    result = runner.invoke(cli, ["--version"])
    assert result.exit_code == 0
    assert result.output == "version 1.0.1a0"


def test_version2():
    runner = CliRunner()
    result = runner.invoke(cli, ["-v"])
    assert result.exit_code == 0
    assert result.output == "version 1.0.1a0"
