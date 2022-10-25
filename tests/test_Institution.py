import csv
import unittest
from datetime import date
from pathlib import Path

import pandas as pd

from src.scripts.agent_table_processing import process_agent_tables
from src.scripts.authenticity_institution import get_QID_inst
from src.scripts.process_Institution import (
    check_each_row_Inst,
    format_year_Inst,
    process_Inst,
)


class MyTestCase(unittest.TestCase):
    # execute before every test case function run.
    def setUp(self):
        THIS_DIR = Path(__file__).parent
        ag_for_test = THIS_DIR / "fixtures/ag.csv"
        pl_for_test = THIS_DIR / "fixtures/pl.csv"
        inst_for_test = THIS_DIR / "fixtures/inst.csv"
        ag = pd.read_csv(ag_for_test).fillna("")
        pl = pd.read_csv(pl_for_test).fillna("")
        inst = pd.read_csv(inst_for_test).fillna("")

        self.today = date.today().strftime("%Y-%m-%d")
        self.ag = ag
        self.pl = pl
        self.inst = inst.astype(str)

        self.all_agents_ids_gh = []
        self.last_inst_id = []
        self.all_agents_ids_gh = []
        self.i = 0

    # execute before every test case function run.
    def tearDown(self):
        del self.ag
        del self.pl
        del self.today
        del self.inst
        del self.i

    def test_it_should_return_QID_info(self):
        self.assertEqual(
            get_QID_inst("August First Film Studio"),
            [{"id": "Q10872932", "label": "August First Film Studio"}],
        )

    def test_it_should_return_None_for_nothing_match(self):
        self.assertEqual(
            get_QID_inst("Jin Opera Group of Liulin County"),
            None,
        )

    def test_it_should_skip(self):
        self.i = 0
        row_for_test = self.inst.iloc[self.i]
        self.all_agents_ids_gh = ["AG0001"]
        self.last_inst_id = ["AG0001"]
        self.all_wikidata_ids = [""]

        row, last_inst_id = check_each_row_Inst(
            self.i,
            row_for_test,
            self.inst,
            self.all_agents_ids_gh,
            self.last_inst_id,
            self.all_wikidata_ids,
        )
        # make the format of start and end (year) valid
        row_new = format_year_Inst(row)
        self.inst.loc[self.i] = row_new
        self.inst.drop("note", axis=1, inplace=True)
        self.assertEqual(
            self.inst.iloc[self.i].tolist(),
            [
                "AG5000",  # inst_id
                "University of Tübingen",  # inst_name
                "en",  # language
                "Tübingen",  # place
                "",  # start
                "",  # end
                "",  # alt_start
                "",  # alt_end
                "Eberhard Karl University of Tübingen",  # inst_alt_name
                "",  # source
                "",  # page
                # note is omitted
                "2022-08-08",  # created
                "QG",  # created_by
                "",  # last_modified
                "",  # last_modified_by
                "Q153978",
            ],  # wikidata_id
        )

    def test_it_should_assert_error_because_no_instID(self):
        self.i = 1
        row_for_test = self.inst.iloc[self.i]
        self.all_agents_ids_gh = ["AG0001"]
        self.last_inst_id = ["AG0001"]
        self.all_wikidata_ids = [""]

        with self.assertRaises(SystemExit) as cm:
            _, _ = check_each_row_Inst(
                self.i,
                row_for_test,
                self.inst,
                self.all_agents_ids_gh,
                self.last_inst_id,
                self.all_wikidata_ids,
            )
        self.assertEqual(cm.exception.code, None)

    def test_it_should_add_start_year(self):
        self.i = 2
        row_for_test = self.inst.iloc[self.i]
        self.all_agents_ids_gh = ["AG0001"]
        self.last_inst_id = ["AG0001"]
        self.all_wikidata_ids = [""]

        row, last_inst_id = check_each_row_Inst(
            self.i,
            row_for_test,
            self.inst,
            self.all_agents_ids_gh,
            self.last_inst_id,
            self.all_wikidata_ids,
        )
        # make the format of start and end (year) valid
        row_new = format_year_Inst(row)
        self.inst.loc[self.i] = row_new
        self.inst.drop("note", axis=1, inplace=True)
        self.assertEqual(
            self.inst.iloc[self.i].tolist(),
            [
                "AG5000",  # inst_id
                "University of Tübingen",  # inst_name
                "en",  # language
                "Tübingen",  # place
                "1477",  # start
                "",  # end
                "",  # alt_start
                "",  # alt_end
                "Eberhard Karl University of Tübingen",  # inst_alt_name
                "",  # source
                "",  # page
                # note is omitted
                "2022-08-08",  # created
                "QG",  # created_by
                self.today,  # last_modified
                "ReadActor",  # last_modified_by
                "Q153978",
            ],  # wikidata_id
        )

    def test_it_should_pass_without_change(self):
        self.i = 3
        row_for_test = self.inst.iloc[self.i]
        self.all_agents_ids_gh = ["AG0001"]
        self.last_inst_id = ["AG0001"]
        self.all_wikidata_ids = [""]

        row, last_inst_id = check_each_row_Inst(
            self.i,
            row_for_test,
            self.inst,
            self.all_agents_ids_gh,
            self.last_inst_id,
            self.all_wikidata_ids,
        )
        # make the format of start and end (year) valid
        row_new = format_year_Inst(row)
        self.inst.loc[self.i] = row_new
        self.inst.drop("note", axis=1, inplace=True)
        self.assertEqual(
            self.inst.iloc[self.i].tolist(),
            [
                "AG5001",  # inst_id
                "A made-up place",  # inst_name
                "en",  # language
                "",  # place
                "",  # start
                "",  # end
                "",  # alt_start
                "",  # alt_end
                "",  # inst_alt_name
                "",  # source
                "",  # page
                # note is omitted
                "2022-08-08",  # created
                "QG",  # created_by
                "",  # last_modified
                "",  # last_modified_by
                "",
            ],  # wikidata_id
        )


if __name__ == "__main__":
    unittest.main()
