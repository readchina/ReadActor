import unittest

import pandas as pd
from command_line_tool import check_each_row, get_last_id


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

    def test1(self):
        self.l = ['AG0001', '鲁', '迅', 'zh', 'male', '0000', '0000', 'Shaoxing', 'Q23114', '2021-12-22', 'QG', '', '', 'skip']
        self.df.loc[0] = self.l
        self.assertEqual(check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                                        self.wikidata_ids_GH)[0].tolist(), ['AG0001', '鲁', '迅', 'zh', 'male', '0000',
                                                                       '0000', 'Shaoxing', 'Q23114', '2021-12-22',
                                                                       'QG', '', '', 'skip'])

    def test1_2(self):

        self.l = ['AG0001', '鲁', '迅', 'zh', 'male', '0000', '1111', 'Shaoxing', 'Q23114', '2021-12-22', 'QG', '', '', '']
        self.df.loc[0] = self.l
        self.assertNotEqual(check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                                        self.wikidata_ids_GH)[0].tolist(), ['AG0001', '鲁', '迅', 'zh', 'male', '1881', '1936', 'Shaoxing', 'Q23114', '2021-12-22', 'QG', '', '', 'skip'])


    def test2(self):
        self.l = ['AG0001', '鲁', '迅', 'zh', 'male', '1881', '1936', 'SP0048', 'Q23114', '2017-07-03', 'LH', '2020-04-02',
             'DP', '']
        self.df.loc[0] = self.l
        self.assertEqual(check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                                        self.wikidata_ids_GH)[0].tolist()[0:-1], ['AG0001', '鲁', '迅', 'zh', 'male', '1881',
                                                                     '1936', 'SP0048', 'Q23114', '2017-07-03', 'LH', '2020-04-02', 'DP'])


    def test3(self):
        self.l = ['AG0001', '鲁', '迅', 'zh', 'male', '1881', '1936', 'Shaoxing', 'Q23114', '2021-12-22', 'QG', '', '', '']
        self.df.loc[0] = self.l
        self.assertEqual(check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                                        self.wikidata_ids_GH)[0].tolist()[0:-1], ['AG0001', '鲁', '迅', 'zh', 'male', '1881',
                                                                     '1936', 'SP0048', 'Q23114', '2017-07-03', 'LH', '2020-04-02', 'DP'])

    def test4(self):
        self.l = ['AG0001', '鲁', '迅', 'zh', 'male', '1881', '1936', 'Shaoxing', 'Q00000000', '2021-12-22', 'QG', '', '', '']
        self.df.loc[0] = self.l
        with self.assertRaises(SystemExit) as cm:
            check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                                        self.wikidata_ids_GH)
        self.assertEqual(cm.exception.code, "For row0 : Error: `wikidata_id` does not match GitHub data. Please "
                                            "check. By SemBot.")


    def test5(self):
        self.l = ['AG1200', 'Monet', 'Claude', 'en', 'male', '1840', '1926', 'Paris', 'Q23114', '2021-12-22', 'QG', '',
             '', '']
        self.df.loc[0] = self.l
        with self.assertRaises(SystemExit) as cm:
            check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                                        self.wikidata_ids_GH)
        self.assertEqual(cm.exception.code, "For row0 :Error: `wikidata_id` already exists in GitHub data but the `person_id` does not match. Please check.")


    def test6(self):
        self.l = ['AG1200', 'Monet', 'Claude', 'en', 'male', '1840', '1926', 'Paris', 'Q296', '2021-12-22', 'QG', '',
             '', '']
        self.df.loc[0] = self.l
        self.assertEqual(check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                                        self.wikidata_ids_GH)[0].tolist()[0:-1], ['AG1200', 'Monet', 'Claude', 'en',
                                                                             'male', '1840', '1926', 'Paris', 'Q296',
                                                                             '2021-12-22', 'QG', '', ''])


    def test7(self):
        self.l = ['AG1200', 'Monet', 'Claude', 'en', '', '', '', '', 'Q296', '2021-12-22', 'QG', '',
             '', '']
        self.df.loc[0] = self.l
        self.assertEqual(check_each_row(0, self.df.iloc[0], self.df_person_gh, self.person_ids_gh, self.last_person_id,
                                        self.wikidata_ids_GH)[0].tolist()[0:-1], ['AG1200', 'Monet', 'Claude', 'en',
                                                                             'male', '1840', '1926', 'Paris', 'Q296', '2021-12-22', 'QG', '', ''])


if __name__ == '__main__':
    unittest.main()
