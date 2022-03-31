import unittest

import pandas as pd
from scripts.command_line_tool import check_each_row, get_last_id


class MyTestCase(unittest.TestCase):
    def setUp(self):
        self.column_names = ['person_id', 'family_name', 'first_name', 'name_lang', 'sex', 'birthyear', 'deathyear',
                             'place_of_birth', 'wikidata_id', 'created', 'created_by',
                             'last_modified', 'last_modified_by', 'note']
        self.l = ['', '', '', '', '', '', '', '', '', '', '', '', '', '']
        self.df = pd.DataFrame([self.l])
        self.df.columns = self.column_names
        self.df_person_gh = pd.read_csv('../CSV/df_person_Github_fake.csv')  # unofficial version
        self.last_person_id, self.person_ids_gh, self.wikidata_ids_GH = get_last_id(self.df_person_gh)

    def test_should_skip_expect_no_change(self):
        self.l = ['AG0001', '鲁', '迅', 'zh', 'male', '0000', '0000', 'Shaoxing', 'Q23114', '2021-12-22', 'QG', '', '',
                  'skip']
        self.df.loc[0] = self.l
        self.assertEqual(check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                                        self.wikidata_ids_GH)[0].tolist(), ['AG0001', '鲁', '迅', 'zh', 'male', '0000',
                                                                            '0000', 'Shaoxing', 'Q23114', '2021-12-22',
                                                                            'QG', '', '', 'skip'])

    def test_should_NOT_skip(self):
        self.l = ['AG0001', '鲁', '迅', 'zh', 'male', '0000', '1111', 'Shaoxing', 'Q23114', '2021-12-22', 'QG', '', '',
                  '']
        self.df.loc[0] = self.l
        self.assertNotEqual(
            check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                           self.wikidata_ids_GH)[0].tolist(),
            ['AG0001', '鲁', '迅', 'zh', 'male', '1881', '1936', 'Shaoxing', 'Q23114', '2021-12-22', 'QG', '', '', ''])

    def test_should_match_all_expect_no_change(self):
        self.l = ['AG0001', '鲁', '迅', 'zh', 'male', '1881', '1936', 'SP0048', 'Q23114', '2017-07-03', 'LH',
                  '2020-04-02',
                  'DP', '']
        self.df.loc[0] = self.l
        self.assertEqual(check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                                        self.wikidata_ids_GH)[0].tolist()[0:-1],
                         ['AG0001', '鲁', '迅', 'zh', 'male', '1881',
                          '1936', 'SP0048', 'Q23114', '2017-07-03', 'LH', '2020-04-02', 'DP'])

    def test_should_two_ids_match_but_other_person_infos_NOT_match_expect_overwrite_with_ReadAct(self):
        self.l = ['AG0001', '鲁', '迅', 'zh', 'male', '1881', '1936', 'Shaoxing', 'Q23114', '2021-12-22', 'QG', '', '',
                  '']
        self.df.loc[0] = self.l
        self.assertEqual(check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                                        self.wikidata_ids_GH)[0].tolist()[0:-1],
                         ['AG0001', '鲁', '迅', 'zh', 'male', '1881',
                          '1936', 'SP0048', 'Q23114', '2017-07-03', 'LH', '2020-04-02', 'DP'])

    def test_should_same_personID_has_different_wikiIds_expect_error(self):
        self.l = ['AG0001', '鲁', '迅', 'zh', 'male', '1881', '1936', 'Shaoxing', 'Q00000000', '2021-12-22', 'QG', '', '',
                  '']
        self.df.loc[0] = self.l
        with self.assertRaises(SystemExit) as cm:
            check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                           self.wikidata_ids_GH)
        self.assertEqual(cm.exception.code, "For row0 : Error: `wikidata_id` does not match GitHub data. Please "
                                            "check. By SemBot.")

    def test_should_personID_NOT_in_ReadAct_but_wikiId_in_ReadAct_expect_error(
            self):
        self.l = ['AG1200', 'Monet', 'Claude', 'en', 'male', '1840', '1926', 'Paris', 'Q23114', '2021-12-22', 'QG', '',
                  '', '']
        self.df.loc[0] = self.l
        with self.assertRaises(SystemExit) as cm:
            check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                           self.wikidata_ids_GH)
        self.assertEqual(cm.exception.code,
                         "For row0 :Error: `wikidata_id` already exists in GitHub data but the `person_id` does not "
                         "match. Please check.")

    def test_should_two_ids_NOT_in_ReadAct_and_person_infos_match_query_expect_no_change(
            self):
        self.l = ['AG1200', 'Monet', 'Claude', 'en', 'male', '1840', '1926', 'Paris', 'Q296', '2021-12-22', 'QG', '',
                  '', '']
        self.df.loc[0] = self.l
        self.assertEqual(check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                                        self.wikidata_ids_GH)[0].tolist()[0:-1], ['AG1200', 'Monet', 'Claude', 'en',
                                                                                  'male', '1840', '1926', 'Paris',
                                                                                  'Q296',
                                                                                  '2021-12-22', 'QG', '', ''])

    def test_should_two_ids_NOT_in_ReadAct_and_person_infos_NOT_match_query_expect_update_unmached_cells(
            self):
        self.l = ['AG1200', 'Monet', 'Claude', 'en', '', '', '', '', 'Q296', '2021-12-22', 'QG', '',
                  '', '']
        self.df.loc[0] = self.l
        self.assertEqual(check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                                        self.wikidata_ids_GH)[0].tolist()[0:-1], ['AG1200', 'Monet', 'Claude', 'en',
                                                                                  'male', '1840', '1926', 'Paris',
                                                                                  'Q296', '2021-12-22', 'QG', '', ''])

    def test_should_queried_WikiID_in_ReadAct_for_new_person_expect_error(
            self):
        self.l = ['AG1200', '鲁', '迅', 'zh', '', '', '', '', '', '2021-12-22', 'QG', '', '', '']
        self.df.loc[0] = self.l
        with self.assertRaises(SystemExit) as cm:
            check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                           self.wikidata_ids_GH)
        self.assertEqual(cm.exception.code, "For row0 :Error: `wikidata_id` queried by family_name, first_name, "
                                            "name_lang already exists in "
                                            "ReadAct data, but your inputted person_id does not match. Please check "
                                            "your data carefully. If you are 100% sure that your input is correct, "
                                            "then it is likely that this person has an identical name with a person "
                                            "in Wikidata database. Please put \"skip\" in \"note\" column for this "
                                            "row and run this program again. By SemBot.")

    def test_should_new_person_has_infos_match_WikiData_info_expect_only_update_WikiId(
            self):
        self.l = ['AG1200', 'Monet', 'Claude', 'en', 'male', '1840', '1926', 'Paris', '', '2021-12-22', 'QG', '',
                  '', '']
        self.df.loc[0] = self.l
        self.assertEqual(check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                                        self.wikidata_ids_GH)[0].tolist()[0:-1], ['AG1200', 'Monet', 'Claude', 'en',
                                                                                  'male', '1840', '1926', 'Paris',
                                                                                  'Q296', '2021-12-22', 'QG', '', ''])

    def should_new_person_has_infos_NOT_match_WikiData_expect_update_with_WikiData(
            self):
        self.l = ['AG1200', 'Monet', 'Claude', 'en', 'male', '1840', '1926', 'Tokyo', '', '2021-12-22', 'QG', '',
                  '', '']
        self.df.loc[0] = self.l
        self.assertEqual(check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                                        self.wikidata_ids_GH)[0].tolist()[0:-1], ['AG1200', 'Monet', 'Claude', 'en',
                                                                                  'male', '1840', '1926', 'Paris',
                                                                                  'Q296',
                                                                                  '2021-12-22', 'QG', '',
                                                                                  ''])  # should test the warning
        # clean up after testing updated files


if __name__ == '__main__':
    unittest.main()
