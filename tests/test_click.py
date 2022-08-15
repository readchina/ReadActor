import datetime
import os
import re
import unittest
from os import listdir
from os.path import exists, isfile, join

import click
from click.testing import CliRunner

from src.scripts.readactor import cli


class TestSum(unittest.TestCase):
    def test_help_1_should_return_documentation(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--help"])
        print(result.output.splitlines())
        assert result.exit_code == 0
        assert result.output.splitlines() == [
            "Usage: cli [OPTIONS] [PATH]",
            "",
            "Options:",
            "  -v, --version      Package version",
            "  -d, --debug        Print full log output to console",
            "  -i, --interactive  Prompt user for confirmation to continue",
            "  -q, --quiet        Print no log output to console other then completion",
            "                     message and error level events",
            "  -o, --output       Do not update input table, but create a new file at <path>",
            "                     instead",
            "  -s, --summary      Do not update input table, but summarise results in console",
            "  -S, --space        Process only places (places and locations)",
            "  -A, --agents       Process only agents (persons and institutions)",
            "  -h, --help         Show this message and exit.",
        ]

    def test_help_2_should_return_documentation(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["-h"])
        print(result.output.splitlines())
        assert result.exit_code == 0
        assert result.output.splitlines() == [
            "Usage: cli [OPTIONS] [PATH]",
            "",
            "Options:",
            "  -v, --version      Package version",
            "  -d, --debug        Print full log output to console",
            "  -i, --interactive  Prompt user for confirmation to continue",
            "  -q, --quiet        Print no log output to console other then completion",
            "                     message and error level events",
            "  -o, --output       Do not update input table, but create a new file at <path>",
            "                     instead",
            "  -s, --summary      Do not update input table, but summarise results in console",
            "  -S, --space        Process only places (places and locations)",
            "  -A, --agents       Process only agents (persons and institutions)",
            "  -h, --help         Show this message and exit.",
        ]

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
        with runner.isolated_filesystem():
            with open("ReadActor.log", "w") as f:
                f.write(
                    "2022-05-24 21:11:23 - root - WARNING: - For row 6, you should input at least a person_id even if "
                    "there is no matched wikidata_id. By SemBot.\n2022-05-24 21:25:48 - root - INFO: - Fields "
                    '"birthyear, deathyear, place_of_birth, created, created_by, last_modified, last_modified_by" '
                    "is/are overwritten.  By SemBot.\n2022-05-24 21:25:48 - urllib3.connectionpool - DEBUG: - "
                    "Starting new HTTPS connection (1): query.wikidata.org:443\n"
                )
            result = runner.invoke(cli, ["--debug"])
            pattern_log = (
                "^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01]) (\d\d:){2}\d{2} - (\w.)+\s-\s("
                "DEBUG|INFO|WARNING|ERROR|CRITICAL|NOTSET):\s-\s(.*?)+"
            )
            assert result.exit_code == 0
            assert bool(re.search(pattern_log, result.output))

    def test_debug_2_should_return_log_text(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            with open("ReadActor.log", "w") as f:
                f.write(
                    "2022-05-24 21:11:23 - root - WARNING: - For row 6, you should input at least a person_id even if "
                    "there is no matched wikidata_id. By SemBot.\n2022-05-24 21:25:48 - root - INFO: - Fields "
                    '"birthyear, deathyear, place_of_birth, created, created_by, last_modified, last_modified_by" '
                    "is/are overwritten.  By SemBot.\n2022-05-24 21:25:48 - urllib3.connectionpool - DEBUG: - "
                    "Starting new HTTPS connection (1): query.wikidata.org:443\n"
                )
            result = runner.invoke(cli, ["-d"])
            pattern_log = (
                "^\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01]) (\d\d:){2}\d{2} - (\w.)+\s-\s("
                "DEBUG|INFO|WARNING|ERROR|CRITICAL|NOTSET):\s-\s(.*?)+"
            )
            assert result.exit_code == 0
            assert bool(re.search(pattern_log, result.output))

    def test_interactive_1_should_be_aborted(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["--interactive", "path"], input="n")
        assert result.exit_code == 1
        assert "Aborted!" in result.output

    def test_interactive_2_should_be_aborted(self):
        runner = CliRunner()
        result = runner.invoke(cli, ["-i", "path"], input="n")
        assert result.exit_code == 1
        assert (
            "Aborted!" in result.output
        )  # (QG) result.output.strip().splitlines() == ['Do you want to update the
        # table? [y/N]: n', 'Aborted!']

    def test_quiet_1_should_not_print_DEBUG(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            with open("test.csv", "w") as f:
                f.write(
                    "1,person_id,family_name,first_name,name_lang,sex,birthyear,deathyear,place_of_birth,wikidata_id,"
                    "created,created_by,last_modified,last_modified_by,note\n2,AG2000,Zhang,San,en,male,1999,,Berlin,"
                    ",2021-12-22,QG,,,skip\n3,AG0001,鲁,迅,zh,male,1881,1936,Shaoxing,Q23114,2021-12-22,QG,,,"
                )
            result = runner.invoke(cli, ["--quiet", "test.csv"])
            assert result.exit_code == 0
            assert (
                "DEBUG" not in result.output
            )  # (QG) Can be rewritten into more detailed assertion like splitlines
            # and then match exact output
            assert (
                "INFO" not in result.output
            )  # (QG) Can be rewritten into more detailed assertion like splitlines
            # and then match exact output

    def test_quiet_2_should_not_print_DEBUG(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            with open("test.csv", "w") as f:
                f.write(
                    "1,person_id,family_name,first_name,name_lang,sex,birthyear,deathyear,place_of_birth,wikidata_id,"
                    "created,created_by,last_modified,last_modified_by,note\n2,AG2000,Zhang,San,en,male,1999,,Berlin,"
                    ",2021-12-22,QG,,,skip\n3,AG0001,鲁,迅,zh,male,1881,1936,Shaoxing,Q23114,2021-12-22,QG,,,"
                )
            result = runner.invoke(cli, ["-q", "test.csv"])
            assert result.exit_code == 0
            assert (
                "DEBUG" not in result.output
            )  # (QG) Can be rewritten into more detailed assertion like splitlines
            # and then match exact output
            assert (
                "INFO" not in result.output
            )  # (QG) Can be rewritten into more detailed assertion like splitlines
            # and then match exact output

    def test_output_1_should_check_new_file(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            with open("Person.csv", "w") as f1:
                f1.write(
                    "1,person_id,family_name,first_name,name_lang,sex,birthyear,deathyear,place_of_birth,wikidata_id,"
                    "created,created_by,last_modified,last_modified_by,note\n2,AG2000,Zhang,San,en,male,1999,,Berlin,"
                    ",2021-12-22,QG,,,skip\n3,AG0001,鲁,迅,zh,male,1881,1936,Shaoxing,Q23114,2021-12-22,QG,,,"
                )
            #     data = {'1':  ['2', '3',],
            #             'person_id': ['AG2000', 'AG0001',],
            #             'family_name': ['Zhang', '鲁',],
            #            'first_name': ['San', '讯',],
            #            'name_lang': ['en', 'zh',],
            #            'sex': ['male', 'male',],
            #            'birthyear': ['1999', '1881',],
            #            'deathyear': ['', '1936',],
            #            'place_of_birth': ['Berlin', 'Shaoxing',],
            #            'wikidata_id': ['', 'Q23114',],
            #            'created': ['2021-12-22', '2021-12-22',],
            #            'created_by': ['QG', 'QG',],
            #            'last_modified': ['', '',],
            #            'last_modified_by': ['', '',],
            #            'note': ['skip', '',]
            # }
            #     df = pd.DataFrame(data)
            #     df.to_csv(f1, index=False)
            result = runner.invoke(cli, ["--output", "Person.csv"])
            assert result.exit_code == 0
            assert exists(
                "Person_updated.csv"
            )  # (QG) check if new file is created or not
            if result.output:
                click.echo(result.output)
            # to get the current working directory
            directory = os.getcwd()
            print(directory)
            onlyfiles = [f for f in listdir(directory) if isfile(join(directory, f))]
            print("onlyfiles: ", onlyfiles)
            with open("Person_updated.csv", "r") as f2:
                update = f2.read()
                print("update: \n", update)
            assert "ReadActor" in update  # (QG) make sure it is not an empty file

    # TODO(QG): it might be more meaningful if this test can be rewritten to raise error and catch the error
    def test_output_2_should_check_new_file(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            with open("Person.csv", "w") as f:
                f.write(
                    "1,person_id,family_name,first_name,name_lang,sex,birthyear,deathyear,place_of_birth,wikidata_id,"
                    "created,created_by,last_modified,last_modified_by,note\n2,AG2000,Zhang,San,en,male,1999,,Berlin,"
                    ",2021-12-22,QG,,,skip\n3,AG0001,鲁,迅,zh,male,1881,1936,Shaoxing,Q23114,2021-12-22,QG,,,"
                )
            result = runner.invoke(cli, ["-o", "Person.csv"])
            directory = os.getcwd()
            with open("Person_updated.csv", "r") as f2:
                update = f2.read()
            assert "ReadActor" in update  # (QG) make sure it is not an empty file

    # def test_summary_1_should_check_new_file(self):
    #     runner = CliRunner()
    #     with runner.isolated_filesystem():
    #         with open("Person.csv", "w") as f1:
    #             file_1 = "1,person_id,family_name,first_name,name_lang,sex,birthyear,deathyear,place_of_birth,wikidata_id,created,created_by,last_modified,last_modified_by,note\n2,AG2000,Zhang,San,en,male,1999,,Berlin,,,2021-12-22,QG,,,skip\n3,AG0001,鲁,迅,zh,male,1881,1936,Shaoxing,Q23114,2021-12-22,QG,,,"
    #             f1.write(
    #                 file_1)
    #         with open("Person.csv", "r") as f2:
    #             file_2 = f2.read()
    #             print("file_content2:\n", file_2)
    #         result = runner.invoke(cli, ["--summary", "-q", "Person.csv"])
    #         print("result.output: \n", result.output)
    #         y = result.output.strip().splitlines()
    #         print("y:\n", y)
    #         assert result.exit_code == 0
    #         assert not exists("Person_updated.csv")
    #         assert file_1 == file_2
    #         assert result.output.strip().splitlines() == ['-------------', 'For row  2  :', "['AG2000', 'Zhang', 'San', 'en', 'male', 1999, '', 'Berlin', '', '', '2021-12-22', 'QG', '', '', 'skip']", '-------------', 'For row  3  :', "['AG0001', '鲁', '迅', 'zh', 'male', 1881, 1930, 'Shaoxing', 'Q23114', '2021-12-22', 'QG', '', '', '', '']", '1,person_id,family_name,first_name,name_lang,sex,birthyear,deathyear,place_of_birth,wikidata_id,created,created_by,last_modified,last_modified_by,note', 'AG2000,Zhang,San,en,male,1999,,Berlin,,,2021-12-22,QG,,,skip', 'AG0001,鲁,迅,zh,male,1881,1936.0,aoxing,Q23114,2021-12-22,QG,,,,']


if __name__ == "__main__":
    unittest.main()
