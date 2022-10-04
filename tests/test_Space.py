import unittest
from datetime import date

import pandas as pd

from src.scripts.authenticity_space import (
    compare_to_openstreetmap,
    get_coordinate_from_wikidata,
    get_QID,
)
from src.scripts.process_Space import process_Spac


class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.column_names = [
            "space_id",
            "old_id",
            "space_type",
            "space_name",
            "language",
            "lat",
            "long",
            "wikidata_id",
            "note",
            "created",
            "created_by",
            "last_modified",
            "last_modified_by",
        ]
        self.l = ["", "", "", "", "", "", "", "", "", "", "", "", ""]
        self.df = pd.DataFrame([self.l])
        self.df.columns = self.column_names
        self.df = self.df.astype(str)
        self.today = date.today().strftime("%Y-%m-%d")

    def test_it_should_return_QID(self):
        self.assertEqual(
            get_QID("Hong Kong"),
            {"id": "Q8646", "label": "Hong Kong"},
        )

    def test_it_should_return_coordinates(self):
        self.assertEqual(
            get_coordinate_from_wikidata("Q8646"),
            [["114.158611111", "22.278333333"]],
        )

    def test_it_should_return_empty_list(self):
        self.assertEqual(
            compare_to_openstreetmap(
                {"SP0023": ["Shenyang", "PL", 41.805699, 123.431472]}
            ),
            [],
        )

    def test_it_should_return_list_of_not_match(self):
        self.assertEqual(
            compare_to_openstreetmap(
                {"SP0025": ["Baiyangdian", "PL", 38.941441, 115.969465]}
            ),
            [["Baiyangdian", "PL", 38.941441, 115.969465, "SP0025"]],
        )

    def test_it_should_respect_skip_annotation(self):
        self.l = [
            "SP3000",
            "",
            "PL",
            "Stuttgart",
            "en",
            "48.7775",
            "9.18",
            "Q1022",
            "skip",
            "QG",
            "2022-10-01",
            "",
            "",
        ]
        self.df.loc[0] = self.l
        self.assertEqual(
            process_Spac(self.df).iloc[0].tolist(),
            [
                "SP3000",
                "",
                "PL",
                "Stuttgart",
                "en",
                "48.7775",
                "9.18",
                "Q1022",
                "skip",
                "QG",
                "2022-10-01",
                "",
                "",
            ],
        )

    def test_it_should_pass_for_available_in_OpenStreetMap(self):
        self.l = [
            "SP3001",
            "",
            "PL",
            "Baden-Württemberg",
            "en",
            "48.5",
            "9.7",
            "",
            "",
            "QG",
            "2022-10-01",
            "",
            "",
        ]
        self.df.loc[0] = self.l
        self.assertEqual(
            process_Spac(self.df).iloc[0].tolist(),
            [
                "SP3001",
                "",
                "PL",
                "Baden-Württemberg",
                "en",
                "48.5",
                "9.7",
                "",
                "",
                "QG",
                "2022-10-01",
                "",
                "",
            ],
        )

    # def test_it_should_pass_for_available_in_OpenStreetMap(self):
    #     self.l = [
    #         "SP3002",
    #         "",
    #         "PL",
    #         "Ancient Culture Museum | Hohentübingen Castle",
    #         "en",
    #         "48.519309594212416",
    #         "9.050910305431827",
    #         "",
    #         "",
    #         "QG",
    #         "2022-10-01",
    #         "",
    #         "",
    #     ]
    #     self.df.loc[0] = self.l
    #     self.assertEqual(
    #         process_Spac(self.df).iloc[0].tolist(),
    #         [
    #             "SP3002",
    #             "",
    #             "PL",
    #             "Ancient Culture Museum | Hohentübingen Castle",
    #             "en",
    #             "48.519309594212416",
    #             "9.050910305431827",
    #             "",
    #             "",
    #             "QG",
    #             "2022-10-01",
    #             "",
    #             "",
    #         ],
    #     )


if __name__ == "__main__":
    unittest.main()
