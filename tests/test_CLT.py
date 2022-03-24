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
        self.df_person_gh = pd.read_csv('CSV/df_person_Github_fake.csv')  # unofficial version
        self.last_person_id, self.person_ids_gh, self.wikidata_ids_GH = get_last_id(self.df_person_gh)

    def test_shoot_skip_expect_no_change(self):
        self.l = ['AG0001', '鲁', '迅', 'zh', 'male', '0000', '0000', 'Shaoxing', 'Q23114', '2021-12-22', 'QG', '', '', 'skip']
        self.df.loc[0] = self.l
        self.assertEqual(check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                                        self.wikidata_ids_GH)[0].tolist(), ['AG0001', '鲁', '迅', 'zh', 'male', '0000',
                                                                       '0000', 'Shaoxing', 'Q23114', '2021-12-22',
                                                                       'QG', '', '', 'skip'])

    def test_shoot_skip_when_no_skip_in_note_expect_processing(self):

        self.l = ['AG0001', '鲁', '迅', 'zh', 'male', '0000', '1111', 'Shaoxing', 'Q23114', '2021-12-22', 'QG', '', '', '']
        self.df.loc[0] = self.l
        self.assertNotEqual(check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                                        self.wikidata_ids_GH)[0].tolist(), ['AG0001', '鲁', '迅', 'zh', 'male', '1881', '1936', 'Shaoxing', 'Q23114', '2021-12-22', 'QG', '', '', ''])


    def test_shoot_same_person_id_same_wikidata_id_same_other_fields_expect_no_change(self):
        self.l = ['AG0001', '鲁', '迅', 'zh', 'male', '1881', '1936', 'SP0048', 'Q23114', '2017-07-03', 'LH', '2020-04-02',
             'DP', '']
        self.df.loc[0] = self.l
        self.assertEqual(check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                                        self.wikidata_ids_GH)[0].tolist()[0:-1], ['AG0001', '鲁', '迅', 'zh', 'male', '1881',
                                                                     '1936', 'SP0048', 'Q23114', '2017-07-03', 'LH', '2020-04-02', 'DP'])


    def test_shoot_same_person_id_same_wikidata_id_different_other_fields_expect_overwrite_with_readact(self):
        self.l = ['AG0001', '鲁', '迅', 'zh', 'male', '1881', '1936', 'Shaoxing', 'Q23114', '2021-12-22', 'QG', '', '', '']
        self.df.loc[0] = self.l
        self.assertEqual(check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                                        self.wikidata_ids_GH)[0].tolist()[0:-1], ['AG0001', '鲁', '迅', 'zh', 'male', '1881',
                                                                     '1936', 'SP0048', 'Q23114', '2017-07-03', 'LH', '2020-04-02', 'DP'])

    def test_shoot_same_person_id_different_wikidata_id_expect_error_wikidata_id_not_in_readact(self):
        self.l = ['AG0001', '鲁', '迅', 'zh', 'male', '1881', '1936', 'Shaoxing', 'Q00000000', '2021-12-22', 'QG', '', '', '']
        self.df.loc[0] = self.l
        with self.assertRaises(SystemExit) as cm:
            check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                                        self.wikidata_ids_GH)
        self.assertEqual(cm.exception.code, "For row0 : Error: `wikidata_id` does not match GitHub data. Please "
                                            "check. By SemBot.")


    def test_shoot_person_id_not_in_readact_but_wikidata_id_in_readact_expect_error_person_id_not_in_readact_or_wrong_info(self):
        self.l = ['AG1200', 'Monet', 'Claude', 'en', 'male', '1840', '1926', 'Paris', 'Q23114', '2021-12-22', 'QG', '',
             '', '']
        self.df.loc[0] = self.l
        with self.assertRaises(SystemExit) as cm:
            check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                                        self.wikidata_ids_GH)
        self.assertEqual(cm.exception.code, "For row0 :Error: `wikidata_id` already exists in GitHub data but the `person_id` does not match. Please check.")


    def test_shoot_person_id_not_in_readact_and_wikidata_id_not_in_readact_expect_and_4_fields_matched_wikidata_query_result_expect_no_change(self):
        self.l = ['AG1200', 'Monet', 'Claude', 'en', 'male', '1840', '1926', 'Paris', 'Q296', '2021-12-22', 'QG', '',
             '', '']
        self.df.loc[0] = self.l
        self.assertEqual(check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                                        self.wikidata_ids_GH)[0].tolist()[0:-1], ['AG1200', 'Monet', 'Claude', 'en',
                                                                             'male', '1840', '1926', 'Paris', 'Q296',
                                                                             '2021-12-22', 'QG', '', ''])


    def test_shoot_person_id_not_in_readact_and_wikidata_id_not_in_readact_expect_but_4_fields_not_matched_wikidata_query_result_expect_update_unmached_cells(self):
        self.l = ['AG1200', 'Monet', 'Claude', 'en', '', '', '', '', 'Q296', '2021-12-22', 'QG', '',
             '', '']
        self.df.loc[0] = self.l
        self.assertEqual(check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                                        self.wikidata_ids_GH)[0].tolist()[0:-1], ['AG1200', 'Monet', 'Claude', 'en',
                                                                             'male', '1840', '1926', 'Paris', 'Q296', '2021-12-22', 'QG', '', ''])


    def test_shoot_person_id_not_in_readact_and_no_input_wikidata_id_and_get_a_wikidata_id_by_query_by_name_and_this_wikidata_id_in_readcact_expect_error_require_check(
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
                                            "in Wikidata database. Please put \"skip\" in \"note\" column for this row and run this program again. By SemBot.")


    def test_shoot_person_id_not_in_readact_and_no_input_wikidata_id_and_get_a_wikidata_id_by_query_by_name_and_this_wikidata_id_not_in_readcact_and_4_fields_match_query_result_expect_no_change(
            self):
        self.l = ['AG1200', 'Monet', 'Claude', 'en', 'male', '1840', '1926', 'Paris', '', '2021-12-22', 'QG', '',
             '', '']
        self.df.loc[0] = self.l
        self.assertEqual(check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                                        self.wikidata_ids_GH)[0].tolist()[0:-1], ['AG1200', 'Monet', 'Claude', 'en',
                                                                             'male', '1840', '1926', 'Paris', 'Q296', '2021-12-22', 'QG', '', ''])


    def test_shoot_person_id_not_in_readact_and_no_input_wikidata_id_and_get_a_wikidata_id_by_query_by_name_and_this_wikidata_id_not_in_readcact_but_4_fields_not_matched_wikidata_query_result_expect_update_unmached_cells(
            self):
        self.l = ['AG1200', 'Monet', 'Claude', 'en', 'male', '1840', '1926', 'Tokyo', '', '2021-12-22', 'QG', '',
             '', '']
        self.df.loc[0] = self.l
        self.assertEqual(check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                                        self.wikidata_ids_GH)[0].tolist()[0:-1], ['AG1200', 'Monet', 'Claude', 'en',
                                                                             'male', '1840', '1926', 'Paris', 'Q296', '2021-12-22', 'QG', '', ''])


if __name__ == '__main__':
    unittest.main()
