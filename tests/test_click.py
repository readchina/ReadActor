import datetime
import re
import unittest
from os.path import exists

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
        with runner.isolated_filesystem():
            with open("ReadActor.log", "w") as f:
                f.write(
                    '2022-05-24 21:11:23 - root - WARNING: - For row 6, you should input at least a person_id even if there is no matched wikidata_id. By SemBot.\n2022-05-24 21:25:48 - root - INFO: - Fields "birthyear, deathyear, place_of_birth, created, created_by, last_modified, last_modified_by" is/are overwritten.  By SemBot.\n2022-05-24 21:25:48 - urllib3.connectionpool - DEBUG: - Starting new HTTPS connection (1): query.wikidata.org:443\n'
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
                    '2022-05-24 21:11:23 - root - WARNING: - For row 6, you should input at least a person_id even if there is no matched wikidata_id. By SemBot.\n2022-05-24 21:25:48 - root - INFO: - Fields "birthyear, deathyear, place_of_birth, created, created_by, last_modified, last_modified_by" is/are overwritten.  By SemBot.\n2022-05-24 21:25:48 - urllib3.connectionpool - DEBUG: - Starting new HTTPS connection (1): query.wikidata.org:443\n'
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
            with open("output.csv", "w") as f:
                f.write(
                    "1,person_id,family_name,first_name,name_lang,sex,birthyear,deathyear,place_of_birth,wikidata_id,"
                    "created,created_by,last_modified,last_modified_by,note\n2,AG2000,Zhang,San,en,male,1999,,Berlin,"
                    ",2021-12-22,QG,,,skip\n3,AG0001,鲁,迅,zh,male,1881,1936,Shaoxing,Q23114,2021-12-22,QG,,,"
                )
            result = runner.invoke(cli, ["--output", "output.csv"])
            assert result.exit_code == 0
            assert exists(
                "output_updated.csv"
            )  # (QG) check if new file is created or not
            assert (
                "last_modified_by" in result.output
            )  # (QG) make sure it is not an empty file

    def test_output_2_should_check_new_file(self):
        runner = CliRunner()
        with runner.isolated_filesystem():
            with open("output.csv", "w") as f:
                f.write(
                    "1,person_id,family_name,first_name,name_lang,sex,birthyear,deathyear,place_of_birth,wikidata_id,"
                    "created,created_by,last_modified,last_modified_by,note\n2,AG2000,Zhang,San,en,male,1999,,Berlin,"
                    ",2021-12-22,QG,,,skip\n3,AG0001,鲁,迅,zh,male,1881,1936,Shaoxing,Q23114,2021-12-22,QG,,,"
                )
            result = runner.invoke(cli, ["-o", "output.csv"])
            assert result.exit_code == 0
            assert exists(
                "output_updated.csv"
            )  # (QG) check if new file is created or not
            assert (
                "last_modified_by" in result.output
            )  # (QG) make sure it is not an empty file

    def test_summary_1_should_check_new_file(self):
        # runner = CliRunner()
        # # with runner.isolated_filesystem():
        # with open("summary.csv", "w") as f:
        #     f.write(
        #         "1,person_id,family_name,first_name,name_lang,sex,birthyear,deathyear,place_of_birth,wikidata_id,"
        #         "created,created_by,last_modified,last_modified_by,note\n3,AG0001,鲁,迅,zh,,,,,Q23114,2021-12-22,QG,,,"
        #     )
        # result = runner.invoke(cli, ["--summary", "-q", "summary.csv"])
        # with open("summary.csv", 'r') as f1:
        #     content = f1.read()
        #     print(content)
        # y = result.output.strip().splitlines()
        # print("######\n", y, "\n")
        # x = ['-------------', 'For row  2  :', "[3, 'AG0001', '鲁', '迅', 'zh', '', '', '', '', 'Q234', '2021-12-22', 'QG', '', '', '']", '1,person_id,family_name,first_name,name_lang,sex,birthyear,deathyear,place_of_birth,wikidata_id,created,created_by,last_modified,last_modified_by,note', '3,AG0001,鲁,迅,zh,male,1881,1936,SP0048,Q23114,2017-07-03,LH,2022-05-03,ReActor," Fields -sex, birthyear, deathyear, place_of_birth, created, created_by, last_modified, last_modified_by- is/are overwritten.  By SemBot."']
        # print("\n", x, "\n")
        # with open("abc.txt", "w") as f2:
        #     for item in x:
        #         f2.write(item + "\n")
        # print(len(x) == len(y))
        # for i in range(len(x)-1):
        #     if x[i] != y[i]:
        #         print("@@@@@@@", i)
        #         print("@@@@@@@", x[i])
        #         print("@@@@@@@", y[i])
        # assert result.exit_code == 0
        # assert not exists("summary_updated.csv")
        # assert result.output.strip().splitlines() == x
        pass


if __name__ == "__main__":
    unittest.main()
