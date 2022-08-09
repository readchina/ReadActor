import unittest
from datetime import date

from src.scripts.authenticity_institution import get_QID_inst


class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.today = date.today().strftime("%Y-%m-%d")

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


if __name__ == "__main__":
    unittest.main()
