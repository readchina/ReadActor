import unittest
from datetime import date

import pandas as pd

from src.scripts.agent_table_processing import preparation, process_agent_tables
from src.scripts.authenticity_institution import get_QID_inst
from src.scripts.process_Institution import check_each_row_Inst, format_year_Inst


class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.inst_columns = [
            "inst_id",
            "inst_name",
            "language",
            "place",
            # Here we use space_name directly. Be aware that we need to replace the space_id with space_name first in
            # the tool
            "start",
            "end",
            "alt_start",
            "alt_end",
            "inst_alt_name",
            "source",
            "page",
            "created",
            "created_by",
            "last_modified",
            "last_modified_by",
            "wikidata_id",  # the original Institution table has no "wikidata_id" column, here we add one for
            # convenience so that we don't need to get wikidata_id from agent table
            "note",
        ]
        self.row_inst = [
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
            "",
        ]
        self.inst = pd.DataFrame([self.row_inst])
        self.inst.columns = self.inst_columns
        self.inst = self.inst.astype(str)
        self.today = date.today().strftime("%Y-%m-%d")

        self.all_agents_ids_gh = []
        self.last_inst_id = []
        self.all_agents_ids_gh = []

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

    def test_it_should_add_infomation(self):
        self.row_inst = [
            "AG2000",
            "Max Planck Society",
            "en",
            "",  # place
            "1948",  # start
            "",  # end
            "",  # alt_start
            "",  # alt_end
            "",  # inst_alt_name
            "",  # source
            "",  # page
            "2022-06-07",
            "QG",
            "",
            "",
            "Q158085",  # wikidata_id
            "",  # note,
        ]
        self.inst.loc[0] = self.row_inst
        self.all_agents_ids_gh = ["AG0002"]
        self.last_inst_id = ["AG0002"]
        self.all_wikidata_ids = [""]

        row, last_inst_id = check_each_row_Inst(
            0,
            self.inst.iloc[0],
            self.inst,
            self.all_agents_ids_gh,
            self.last_inst_id,
            self.all_wikidata_ids,
        )
        # make the format of start and end (year) valid
        row = format_year_Inst(row)
        print("row: ", row)
        # print(row.tolist()[0:-1])
        self.assertEqual(
            row.tolist()[0:-1],
            [
                "AG2000",
                "Max Planck Society",
                "en",
                "Munich",  # place
                "1948",  # start
                "",  # end
                "",  # alt_start
                "",  # alt_end
                "",  # inst_alt_name
                "",  # source
                "",  # page
                "2022-06-07",
                "QG",
                self.today,
                "ReadActor",
                "Q158085",
            ],
        )

    def test_it_should_not_change_with_unspecific_year_format(self):
        self.row_inst = [
            "AG3000",
            "University Germany",
            "en",
            "Germany",  # place
            "197X",  # start
            "",  # end
            "",  # alt_start
            "",  # alt_end
            "",  # inst_alt_name
            "",  # source
            "",  # page
            "2019-06-15",
            "WH",
            "",
            "",
            "",  # wikidata_id
            "",  # note,
        ]
        self.inst.loc[0] = self.row_inst
        self.all_agents_ids_gh = ["AG3000"]
        self.last_inst_id = ["AG3000"]
        self.all_wikidata_ids = [""]

        row, last_inst_id = check_each_row_Inst(
            0,
            self.inst.iloc[0],
            self.inst,
            self.all_agents_ids_gh,
            self.last_inst_id,
            self.all_wikidata_ids,
        )
        # make the format of start and end (year) valid
        row = format_year_Inst(row)
        print("row: ", row)
        # print(row.tolist()[0:-1])
        self.assertEqual(
            row.tolist()[0:-1],
            [
                "AG3000",
                "University Germany",
                "en",
                "Germany",  # place
                "197X",  # start
                "",  # end
                "",  # alt_start
                "",  # alt_end
                "",  # inst_alt_name
                "",  # source
                "",  # page
                "2019-06-15",
                "WH",
                "",
                "",
                "",
            ],
        )


if __name__ == "__main__":
    unittest.main()
