import unittest
from datetime import date

import pandas as pd

from src.scripts.agent_table_processing import process_agent_tables
from src.scripts.process_Person import check_each_row_Person


class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.column_names = [
            "person_id",
            "family_name",
            "first_name",
            "language",
            "sex",
            "birthyear",
            "deathyear",
            "place_of_birth",
            "wikidata_id",
            "created",
            "created_by",
            "last_modified",
            "last_modified_by",
            "note",
        ]
        self.l = ["", "", "", "", "", "", "", "", "", "", "", "", "", ""]
        self.df = pd.DataFrame([self.l])
        self.df.columns = self.column_names
        self.df = self.df.astype(str)

        (
            self.df_person_new,
            self.person_ids_gh,
            self.last_person_id,
            self.wikidata_ids_GH,
        ) = process_agent_tables("Person", "ReadAct", [])
        self.today = date.today().strftime("%Y-%m-%d")

    def test_it_should_respect_skip_annotation(self):
        self.l = [
            "AG0001",
            "鲁",
            "迅",
            "zh",
            "male",
            "0000",
            "0000",
            "Shaoxing",
            "Q23114",
            "2021-12-22",
            "QG",
            "",
            "",
            "skip",
        ]
        self.df.loc[0] = self.l
        self.assertEqual(
            check_each_row_Person(
                0,
                self.df.iloc[0],
                self.df_person_new,
                self.person_ids_gh,
                self.last_person_id,
                self.wikidata_ids_GH,
            )[0].tolist(),
            [
                "AG0001",
                "鲁",
                "迅",
                "zh",
                "male",
                "0000",
                "0000",
                "Shaoxing",
                "Q23114",
                "2021-12-22",
                "QG",
                "",
                "",
                "skip",
            ],
        )

    def test_it_should_NOT_skip_without_annotation(self):
        self.l = [
            "AG0001",
            "鲁",
            "迅",
            "zh",
            "male",
            "0000",
            "1111",
            "Shaoxing",
            "Q23114",
            "2021-12-22",
            "QG",
            "",
            "",
            "",
        ]
        self.df.loc[0] = self.l
        self.assertNotEqual(
            check_each_row_Person(
                0,
                self.df.iloc[0],
                self.df_person_new,
                self.person_ids_gh,
                self.last_person_id,
                self.wikidata_ids_GH,
            )[0].tolist()[0:-1],
            [
                "AG0001",
                "鲁",
                "迅",
                "zh",
                "male",
                "1881",
                "1936",
                "Shaoxing",
                "Q23114",
                "2021-12-22",
                "QG",
                self.today,
                "ReadActor",
            ],
        )

    # def test_it_should_match_all_witout_change(self):
    #     self.l = [
    #         "AG0001",
    #         "鲁",
    #         "迅",
    #         "zh",
    #         "male",
    #         "1881",
    #         "1936",
    #         "SP0048",
    #         "Q23114",
    #         "2017-07-03",
    #         "LH",
    #         "2022-05-03",
    #         "ReadActor",
    #         "",
    #     ]
    #     self.df.loc[0] = self.l
    #     self.assertEqual(
    #         check_each_row_Person(
    #             0,
    #             self.df.iloc[0],
    #             self.df_person_new,
    #             self.person_ids_gh,
    #             self.last_person_id,
    #             self.wikidata_ids_GH,
    #         )[0].tolist()[0:-1],
    #         [
    #             "AG0001",
    #             "鲁",
    #             "迅",
    #             "zh",
    #             "male",
    #             "1881",
    #             "1936",
    #             "SP0048",
    #             "Q23114",
    #             "2017-07-03",
    #             "LH",
    #             "2022-05-03",
    #             "ReadActor",
    #         ],
    #     )

    # def test_it_should_update_data_for_full_id_match(self):
    #     self.l = [
    #         "AG0001",
    #         "鲁",
    #         "迅",
    #         "zh",
    #         "male",
    #         "1881",
    #         "1936",
    #         "Shaoxing",
    #         "Q23114",
    #         "2021-12-22",
    #         "QG",
    #         "",
    #         "",
    #         "",
    #     ]
    #     self.df.loc[0] = self.l
    #     self.assertEqual(
    #         check_each_row_Person(
    #             0,
    #             self.df.iloc[0],
    #             self.df_person_new,
    #             self.person_ids_gh,
    #             self.last_person_id,
    #             self.wikidata_ids_GH,
    #         )[0].tolist()[0:-1],
    #         [
    #             "AG0001",
    #             "鲁",
    #             "迅",
    #             "zh",
    #             "male",
    #             "1881",
    #             "1936",
    #             "SP0048",
    #             "Q23114",
    #             "2017-07-03",
    #             "LH",
    #             "2022-05-03",
    #             "ReadActor",
    #         ],
    #     )

    def test_it_should_fail_on_ID_conflict(self):
        self.l = [
            "AG0001",
            "鲁",
            "迅",
            "zh",
            "male",
            "1881",
            "1936",
            "Shaoxing",
            "Q00000000",
            "2021-12-22",
            "QG",
            "",
            "",
            "",
        ]
        self.df.loc[0] = self.l
        with self.assertRaises(SystemExit) as cm:
            check_each_row_Person(
                0,
                self.df.iloc[0],
                self.df_person_new,
                self.person_ids_gh,
                self.last_person_id,
                self.wikidata_ids_GH,
            )
        self.assertEqual(cm.exception.code, None)

    def test_it_should_not_reuse_wikiID(self):
        self.l = [
            "AG1200",
            "Monet",
            "Claude",
            "en",
            "male",
            "1840",
            "1926",
            "Paris",
            "Q23114",
            "2021-12-22",
            "QG",
            "",
            "",
            "",
        ]
        self.df.loc[0] = self.l
        with self.assertRaises(SystemExit) as cm:
            check_each_row_Person(
                0,
                self.df.iloc[0],
                self.df_person_new,
                self.person_ids_gh,
                self.last_person_id,
                self.wikidata_ids_GH,
            )
        self.assertEqual(cm.exception.code, None)

    # TODO(DP): I do not understand how this testcase is different from the second test above
    # (QG): This should be a new entry which does not exist in ReadAct. The second test above is for an existed person.
    def test_it_should_not_update_new_entries_with_matching_data(self):
        self.l = [
            "AG0000",
            "Musk",
            "Elon",
            "en",
            "male",
            "1971",
            "",
            "Pretoria",
            "Q317521",
            "2022-06-07",
            "QG",
            "",
            "",
            "",
        ]
        self.df.loc[0] = self.l
        self.assertEqual(
            check_each_row_Person(
                0,
                self.df.iloc[0],
                self.df_person_new,
                self.person_ids_gh,
                self.last_person_id,
                self.wikidata_ids_GH,
            )[0].tolist()[0:-1],
            [
                "AG0000",
                "Musk",
                "Elon",
                "en",
                "male",
                "1971",
                "",
                "Pretoria",
                "Q317521",
                "2022-06-07",
                "QG",
                "",
                "",
            ],
        )

    def test_it_should_update_new_entries_with_missing_data(self):
        self.l = [
            "AG0000",
            "Musk",
            "Elon",
            "en",
            "",
            "",
            "",
            "",
            "Q317521",
            "2022-06-07",
            "QG",
            "",
            "",
            "",
        ]
        self.df.loc[0] = self.l
        self.assertEqual(
            check_each_row_Person(
                0,
                self.df.iloc[0],
                self.df_person_new,
                self.person_ids_gh,
                self.last_person_id,
                self.wikidata_ids_GH,
            )[0].tolist()[0:-1],
            [
                "AG0000",
                "Musk",
                "Elon",
                "en",
                "male",
                "1971",
                "",
                "Pretoria",
                "Q317521",
                "2022-06-07",
                "QG",
                self.today,
                "ReadActor",
            ],
        )

    def test_it_should_use_wikiID_to_dedupe_readActID(self):
        self.l = [
            "AG1200",
            "鲁",
            "迅",
            "zh",
            "",
            "",
            "",
            "",
            "",
            "2021-12-22",
            "QG",
            "",
            "",
            "",
        ]
        self.df.loc[0] = self.l
        with self.assertRaises(SystemExit) as cm:
            check_each_row_Person(
                0,
                self.df.iloc[0],
                self.df_person_new,
                self.person_ids_gh,
                self.last_person_id,
                self.wikidata_ids_GH,
            )
        self.assertEqual(cm.exception.code, None)

    def test_it_should_update_wikiID_using_persondata(self):
        self.l = [
            "AG0000",
            "Musk",
            "Elon",
            "en",
            "male",
            "1971",
            "",
            "Pretoria",
            "",
            "2022-06-07",
            "QG",
            "",
            "",
            "",
        ]
        self.df.loc[0] = self.l
        self.assertEqual(
            check_each_row_Person(
                0,
                self.df.iloc[0],
                self.df_person_new,
                self.person_ids_gh,
                self.last_person_id,
                self.wikidata_ids_GH,
            )[0].tolist()[0:-1],
            [
                "AG0000",
                "Musk",
                "Elon",
                "en",
                "male",
                "1971",
                "",
                "Pretoria",
                "Q317521",
                "2022-06-07",
                "QG",
                self.today,
                "ReadActor",
            ],
        )

    def test_it_should_update_data_with_missing_wikiID(self):
        self.l = [
            "AG0000",
            "Musk",
            "Elon",
            "en",
            "male",
            "1971",
            "",
            "Tokyo",
            "",
            "2022-06-07",
            "QG",
            "",
            "",
            "",
        ]
        self.df.loc[0] = self.l
        self.assertEqual(
            check_each_row_Person(
                0,
                self.df.iloc[0],
                self.df_person_new,
                self.person_ids_gh,
                self.last_person_id,
                self.wikidata_ids_GH,
            )[0].tolist()[0:-1],
            [
                "AG0000",
                "Musk",
                "Elon",
                "en",
                "male",
                "1971",
                "",
                "Pretoria",
                "Q317521",
                "2022-06-07",
                "QG",
                self.today,
                "ReadActor",
            ],
        )
        # should test the warning and clean up after testing updated files


if __name__ == "__main__":
    unittest.main()
