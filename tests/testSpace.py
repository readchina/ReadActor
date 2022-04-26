import unittest
from datetime import date

from src.scripts.authenticity_space import (
    compare_to_openstreetmap,
    get_coordinate_from_wikidata,
    get_QID,
)


class MyTestCase(unittest.TestCase):
    def setUp(self):
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
            [["Baiyangdian", "PL", 38.941441, 115.969465]],
        )


if __name__ == "__main__":
    unittest.main()
