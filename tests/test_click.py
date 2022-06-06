import re
import unittest

from click.testing import CliRunner

from src.scripts.readactor import cli


class TestSum(unittest.TestCase):
    def test_version1(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--version"])
        assert result.exit_code == 0
        assert bool(
            re.search("version (\d+\.){2}(\d)+((-|_))*(\w)*", result.output.strip())
        )

    def test_version2(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["-v"])
        assert result.exit_code == 0
        assert bool(
            re.search("version (\d+\.){2}(\d)+((-|_))*(\w)*", result.output.strip())
        )


if __name__ == "__main__":
    unittest.main()
