import re
import unittest

from click.testing import CliRunner

from src.scripts.readactor import cli


class TestSum(unittest.TestCase):
    def test_version_1_should_return_version(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert bool(
            re.search("version (\d+\.){2}(\d)+((-|_))*(\w)*", result.output.strip())
        )

    def test_version_2_should_return_version(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["-v"])
        assert result.exit_code == 0
        assert bool(
            re.search("version (\d+\.){2}(\d)+((-|_))*(\w)*", result.output.strip())
        )

    def test_debug_1_should_return_log_text(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--debug"])
        pattern_log = (
            "^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01]) (\d\d:){2}\d{2} - (\w.)+\s-\s(DEBUG|INFO|WARNING|ERROR|CRITICAL|NOTSET):\s-\s(.*?)+"
        )
        assert result.exit_code == 0
        assert bool(re.search(pattern_log, result.output))

    def test_debug_2_should_return_log_text(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["-d"])
        pattern_log = (
            "^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01]) (\d\d:){2}\d{2} - (\w.)+\s-\s(DEBUG|INFO|WARNING|ERROR|CRITICAL|NOTSET):\s-\s(.*?)+"
        )
        assert result.exit_code == 0
        assert bool(re.search(pattern_log, result.output))


if __name__ == "__main__":
    unittest.main()
