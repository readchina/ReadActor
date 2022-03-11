import argparse
import sys
import time
import pandas as pd
import logging

from scripts.authenticity_person import order_name_by_language, get_Qid_from_wikipedia_url, sparql_with_Qid, \
    sparql_by_name

DATA_DICTIONARY_GITHUG = "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data_dictionary.csv"
PERSON_CSV_GITHUB = "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Person.csv"
EXPECTED_COL = ['person_id', 'family_name', 'first_name', 'name_lang', 'sex', 'birthyear', 'deathyear',
                'place_of_birth', 'wikidata_id', 'created', 'created_by',
                'last_modified', 'last_modified_by', 'note']
MINIMAL_COL = ['family_name', 'first_name', 'name_lang']
FIELDS_OF_WIKIDATA = ['sex', 'birthyear', 'deathyear',
                      'place_of_birth']  # gender, birthplace


# def update(df, dict):
#     flag = False
#     if 'gender' in dict:
#         df.at[index, 'sex'] = dict['gender']
#         flag = True
#     if 'birthyear' in dict:
#         df.at[index, 'birthyear'] = dict['birthyear']
#         flag = True
#     if 'deathyear' in dict:
#         df.at[index, 'deathyear'] = dict['deathyear']
#         flag = True
#     if 'place_of_birth' in dict:
#         df.at[index, 'place_of_birth'] = dict['place_of_birth']
#         flag = True
#     if flag:
#         df.at[index, 'last_modified'] = time.strftime("%Y-%m-%d", time.localtime())
#         df.at[index, 'last_modified_by'] = 'SemBot'
#     return df


def validate(path='../CSV/Person.csv'):
    valid = False
    df = pd.read_csv(path, index_col=0)
    df = df.fillna('')  # Replace all the nan into empty string

    if not set(MINIMAL_COL).issubset(df.columns.tolist()):
        print('Your file is lacking of the following minimal mandatory column(s):')
        print(set(MINIMAL_COL) - set(df.columns.tolist()))
        # TO DO: here needs to write information in the error log
    elif not set(EXPECTED_COL).issubset(set(df.columns.tolist())):
        missing_columns = set(EXPECTED_COL) - set(df.columns.tolist())
        valid = True
        print('There are 15 expected columns in Person.csv.\nYour file has missing column(s):')
        print(list(missing_columns))
        for i in missing_columns:
            df[i] = ""
        df = df[['person_id', 'family_name', 'first_name', 'name_lang', 'sex', 'birthyear', 'deathyear',
                 'place_of_birth', 'wikidata_id', 'created', 'created_by',
                 'last_modified', 'last_modified_by', 'note']]
        print("All the missing columns are inserted to your csv table now.\nNote that columns outside the 15 expected "
              "columns are dropped.")
        # To Do: rewrite to make sure that each column has a fixed position in any Person.csv
    else:
        df = df[['person_id', 'family_name', 'first_name', 'name_lang', 'sex', 'birthyear', 'deathyear',
                 'place_of_birth', 'wikidata_id', 'created', 'created_by',
                 'last_modified', 'last_modified_by', 'note']]
        valid = True
        print("--> Validate 2/2 \nAll 15 expected columns are included.\nPlease note that any irrelevant column will "
              "be "
              "dropped.\n")
    return valid, df


def __compare_two_rows(row, row_gh):
    """
    This function will be triggered when `person_id` and `wikidata_id` are the same.
    It will compare the rest fields of two rows from two dataframes seperately.

    :param row: one row from the user-uploaded Person.csv
    :param row_gh: the row in Person.csv on GitHub which has the same `person_id` and `wikidata_id` with the other
    parameter
    :return: True if all are matching, otherwise False
    """
    fields_to_be_compared = ['family_name', 'first_name', 'name_lang', 'sex', 'birthyear', 'deathyear',
                             'place_of_birth', 'created', 'created_by',
                             'last_modified', 'last_modified_by', 'note']
    for i in fields_to_be_compared:
        if row[i] != row_gh[i]:
            return False
    return True


def __overwrite(row, row_gh):
    """
    This function will overwrite all the fields except `person_id` and `wikidata_id`.
    :param row: one row from the user-uploaded Person.csv
    :param row_gh: the row in Person.csv on GitHub which has the same `person_id` and `wikidata_id` with the other
    parameter
    :return: row which is modified
    """
    fields_to_be_overwritten = ['family_name', 'first_name', 'name_lang', 'sex', 'birthyear', 'deathyear',
                                'place_of_birth', 'created', 'created_by',
                                'last_modified', 'last_modified_by', 'note']
    note_flag = False
    modified_fields = []
    for i in fields_to_be_overwritten:
        if row[i] != row_gh[i]:
            row[i] = row_gh[i]
            modified_fields.append(i)
            note_flag = True
    if note_flag:
        if isinstance(row['note'], str):
            row['note'] = row['note'] + ' Fields "' + ", ".join(modified_fields) + '" in this table is/are ' \
                                                                                   'overwritten.  By SemBot.'
        else:
            row['note'] = 'Fields "' + ", ".join(modified_fields) + '" is/are overwritten.  By SemBot.'
    # Todo: log
    return row


def __compare_wikidata_ids(index, row, df_person_GH):
    wikidata_id_usr = row['wikidata_id']
    row_gh_index = df_person_GH.index[(df_person_GH['person_id'] == row['person_id']) & (
            df_person_GH['name_lang'] == row['name_lang'])].tolist()[0]
    row_GH = df_person_GH.iloc[row_gh_index]
    wikidata_id_gh = row_GH['wikidata_id']
    # print("wikidata_id_gh: ", wikidata_id_gh)
    # print("wikidata_id_usr: ", wikidata_id_usr)
    if wikidata_id_gh == wikidata_id_usr:
        res = __compare_two_rows(row, row_GH)
        if not res:
            return __overwrite(row, row_GH)
        # Todo: Here should edit log info: "Row ... checked. Pass."
        return row
    else:
        row['note'] = 'Error: `wikidata_id` is not matching with GitHub data. Please check. By SemBot.'
        # print('For row', index, ' :', 'Error: `wikidata_id` does not match GitHub data. Please check. By SemBot.')
        # Todo: Here should edit log info: "Error..."
        error_msg = 'For row' + str(index) + ' : Error: `wikidata_id` does not match GitHub data. Please check. By ' \
                                             'SemBot.'
        sys.exit(error_msg)


def __check_person_id_size(last_id_in_gh):
    if int(last_id_in_gh[2:]) >= 9999:
        print("Warning: It is better to update all person_id in the database. By SemBot.")
        if isinstance(row['note'], str):
            row['note'] = row['note'] + ' Warning: It is better to update all person_id in the database. By SemBot.'
        else:
            row['note'] = 'Warning: It is better to update all person_id in the database. By SemBot.'


def check_gh(df):  # a function to check if Person.csv on GitHub has `wikidata_id` column
    if 'wikidata_id' not in df.columns:
        # print('There is no `wikidata_id` column in the Person.csv on GitHub. Please inform someone to '
        #       'check it. By SemBot.')
        exit('There is no `wikidata_id` column in the Person.csv on GitHub. Please inform someone to '
             'check it. By SemBot.')


def get_last_id(df):
    person_ids_GH = df['person_id'].tolist()
    person_ids_GH.sort()
    wikidata_ids_GH = df['wikidata_id'].tolist()
    return person_ids_GH[-1], person_ids_GH, wikidata_ids_GH


def check_each_row(index, row, df_person_gh, person_ids_gh, last_person_id, wikidata_ids_GH):
    if row['note'] == 'skip' or row['note'] == 'Skip':
        return row, last_person_id
    else:
        if isinstance(row['person_id'], str) and len(row['person_id']) > 0:
            if row['person_id'] in person_ids_gh:
                return __compare_wikidata_ids(index, row, df_person_gh), last_person_id
            else:
                if (isinstance(row['wikidata_id'], str) is True) and (len(row['wikidata_id']) > 0):
                    if row['wikidata_id'] in wikidata_ids_GH:
                        row[
                            'note'] = 'Error: `wikidata_id` already exists in GitHub data but the person_id does not match. Please check. By SemBot.'
                        # print('For row', index, ' :', 'Error: `wikidata_id` already exists in GitHub data but the '
                        #                               '`person_id` does not match. Please check.')
                        # Todo: Here should edit log info: "Error..."
                        error_msg = 'For row' + str(
                            index) + ' :' + 'Error: `wikidata_id` already exists in GitHub data ' \
                                            'but the `person_id` does not match. Please check.'
                        sys.exit(error_msg)
                    else:
                        wikidata_id_usr = row['wikidata_id']
                        person_dict = sparql_with_Qid(wikidata_id_usr)
                        note_flag = False
                        modified_fields = []
                        if 'gender' in person_dict and row['sex'] != person_dict['gender']:
                            row['sex'] = person_dict['gender']
                            modified_fields.append('sex')
                            note_flag = True
                        if 'birthyear' in person_dict and row['birthyear'] != person_dict['birthyear']:
                            row['birthyear'] = person_dict['birthyear']
                            modified_fields.append('birthyear')
                            note_flag = True
                        if 'deathyear' in person_dict and row['deathyear'] != person_dict['deathyear']:
                            row['deathyear'] = person_dict['deathyear']
                            modified_fields.append('deathyear')
                            note_flag = True
                        if 'birthplace' in person_dict and row['place_of_birth'] != person_dict['birthplace']:
                            row['place_of_birth'] = person_dict['birthplace']
                            modified_fields.append('place_of_birth')
                            note_flag = True
                        if note_flag:
                            if isinstance(row['note'], str):
                                row['note'] = row['note'] + ' Fields "' + ", ".join(
                                    modified_fields) + '" in this table is/are ' \
                                                       'overwritten.  By SemBot.'
                            else:
                                row['note'] = 'Fields "' + ", ".join(
                                    modified_fields) + '" is/are overwritten.  By SemBot.'
                            # Todo Log
                            return row, last_person_id
                        else:
                            # Todo Log: pass.
                            return row, last_person_id
                else:  # user inputted "person_id" but not "wikidata_id"
                    names = order_name_by_language(row)
                    person = sparql_by_name(names, row['name_lang'], 2)
                    if len(person) > 0:
                        wikidata_id_usr = next(iter(person))
                        if wikidata_id_usr in wikidata_ids_GH:
                            row[
                                'note'] = 'Error: `wikidata_id` queried by family_name, first_name, name_lang already exists in ReadAct data, but your inputted person_id does not match. Please check your data carefully. If you are 100% sure that your input is correct, then it is likely that this person has an identical name with a person in Wikidata database. Please put "skip" in "note" column for this row and run this program again. By SemBot.'
                            # print('For row', index, ' :', 'Error: `wikidata_id` queried by family_name, first_name, name_lang already exists in ReadAct data, but your inputted person_id does not match. Please check your data carefully. If you are 100% sure that your input is correct, then it is likely that this person has an identical name with a person in Wikidata database. Please put "skip" in "note" column for this row and run this program again. By SemBot.')
                            # Todo: Here should edit log info: "Error..."
                            error_msg = 'For row' + str(index) + ' :' + 'Error: `wikidata_id` queried by family_name, ' \
                                                                        'first_name, name_lang already exists in ReadAct data, but your inputted person_id does not match. Please check your data carefully. If you are 100% sure that your input is correct, then it is likely that this person has an identical name with a person in Wikidata database. Please put "skip" in "note" column for this row and run this program again. By SemBot.'
                            sys.exit(error_msg)
                        else:
                            row['wikidata_id'] = wikidata_id_usr
                            person_dict = sparql_with_Qid(wikidata_id_usr)
                            note_flag = False
                            modified_fields = ['wikidata_id']
                            if 'gender' in person_dict and row['sex'] != person_dict['gender']:
                                modified_fields.append('sex')
                                note_flag = True
                            if 'birthyear' in person_dict and row['birthyear'] != person_dict['birthyear']:
                                modified_fields.append('birthyear')
                                note_flag = True
                            if 'deathyear' in person_dict and row['deathyear'] != person_dict['deathyear']:
                                modified_fields.append('deathyear')
                                note_flag = True
                            if 'birthplace' in person_dict and row['place_of_birth'] != person_dict['birthplace']:
                                modified_fields.append('place_of_birth')
                                note_flag = True
                            if note_flag:
                                if isinstance(row['note'], str):
                                    row['note'] = row['note'] + ' Fields "' + ", ".join(
                                        modified_fields) + '" in this table is/are updated.  By SemBot.'
                                else:
                                    row['note'] = 'Fields "' + ", ".join(
                                        modified_fields) + '" is/are updated.  By SemBot.'
                                print("Warning: You should look row", index,
                                      " up in Wikidata again. If it does not match with "
                                      "this modification, you should retrieve the old data for this row and put 'skip' in 'note'.")
                                # Todo Log: warning
                                return row, last_person_id
                            else:
                                print("Warning: You should look the person in row ", index,
                                      " up in Wikidata and input the "
                                      "`wikidata_id` in your table in the future.")
                                # Todo Log: Warning.
                                if isinstance(row['note'], str):
                                    row['note'] = row[
                                                      'note'] + ' Field `wikidata_id` in this table is updated.  By SemBot.'
                                else:
                                    row['note'] = 'Field `wikidata_id` in this table is updated.  By SemBot.'
                                return row, last_person_id
                    else:
                        if isinstance(row['note'], str):
                            row['note'] = row['note'] + ' No match in Wikidata.  By SemBot.'
                        else:
                            row['note'] = 'No match in Wikidata.  By SemBot.'
                        # Todo log: info: checked. All right.
                        return row, last_person_id
        else:  # No user inputted `person_id`
            __check_person_id_size(last_person_id)
            row['person_id'] = last_person_id[0:2] + str(int(last_person_id[2:]) + 1)
            if (isinstance(row['wikidata_id'], str) is True) and (
                    len(row['wikidata_id']) > 0):  # no person_id, but has wikidata_id
                if row['wikidata_id'] in wikidata_ids_GH:
                    row[
                        'note'] = 'Error: this `wikidata_id` already exists in ReadAct. Please check carefully. If you are 100% sure that your input is correct, then it is likely that this person has an identical name with a person in Wikidata database. Please put "skip" in "note" column for this row and run this program again.  By SemBot.'
                    # print('For row', index, ' :', 'Error: this `wikidata_id` already exists in ReadAct. Please check carefully. If you are 100% sure that your input is correct, then it is likely that this person has an identical name with a person in Wikidata database. Please put "skip" in "note" column for this row and run this program again.  By SemBot.')
                    # Todo: log info: "Error..."
                    error_msg = 'For row'+ str(index) + ' : Error: this `wikidata_id` already exists in ReadAct. Please check carefully. If you are 100% sure that your input is correct, then it is likely that this person has an identical name with a person in Wikidata database. Please put "skip" in "note" column for this row and run this program again.  By SemBot.'
                    sys.exit(error_msg)
                else:
                    last_person_id = row['person_id']
                    person_dict = sparql_with_Qid(row['wikidata_id'])
                    note_flag = False
                    modified_fields = ['person_id']
                    if 'gender' in person_dict and row['sex'] != person_dict['gender']:
                        modified_fields.append('sex')
                        note_flag = True
                    if 'birthyear' in person_dict and row['birthyear'] != person_dict['birthyear']:
                        modified_fields.append('birthyear')
                        note_flag = True
                    if 'deathyear' in person_dict and row['deathyear'] != person_dict['deathyear']:
                        modified_fields.append('deathyear')
                        note_flag = True
                    if 'birthplace' in person_dict and row['place_of_birth'] != person_dict['birthplace']:
                        modified_fields.append('place_of_birth')
                        note_flag = True
                    if note_flag:
                        if isinstance(row['note'], str):
                            row['note'] = row['note'] + ' Fields "' + ", ".join(
                                modified_fields) + '" in this table is/are updated.  By SemBot.'
                        else:
                            row['note'] = 'Fields "' + ", ".join(modified_fields) + '" is/are updated.  By SemBot.'
                        # Todo Log: info.
                        return row, last_person_id
                    else:
                        if isinstance(row['note'], str):
                            row['note'] = row['note'] + ' Field `person_id` in this table is updated.  By SemBot.'
                        else:
                            row['note'] = 'Field `person_id` in this table is updated.  By SemBot.'
                        # Todo Log: Info.
                        return row, last_person_id
            else:  # no person_id, no wikidata_id
                names = order_name_by_language(row)
                person = sparql_by_name(names, row['name_lang'], 2)
                if len(person) > 0:
                    wikidata_id_usr = next(iter(person))
                    if wikidata_id_usr in wikidata_ids_GH:
                        row[
                            'note'] = 'Error: `wikidata_id` queried by family_name, first_name, name_lang already exists in ReadAct data, but your inputted person_id does not match. Please check your data carefully. If you are 100% sure that your input is correct, then it is likely that this person has an identical name with a person in Wikidata database. Please put "skip" in "note" column for this row and run this program again. By SemBot.'
                        # print('For row', index, ' :', 'Error: `wikidata_id` queried by family_name, first_name, name_lang already exists in ReadAct data, but your inputted person_id does not match. Please check your data carefully. If you are 100% sure that your input is correct, then it is likely that this person has an identical name with a person in Wikidata database. Please put "skip" in "note" column for this row and run this program again. By SemBot.')
                        # Todo: Here should edit log info: "Error..."
                        error_msg = 'For row'+ str(index) + ' : Error: `wikidata_id` queried by family_name, ' \
                                                             'first_name, name_lang already exists in ReadAct data, but your inputted person_id does not match. Please check your data carefully. If you are 100% sure that your input is correct, then it is likely that this person has an identical name with a person in Wikidata database. Please put "skip" in "note" column for this row and run this program again. By SemBot.'
                        sys.exit(error_msg)
                    else:
                        last_person_id = row['person_id']
                        row['wikidata_id'] = wikidata_id_usr
                        person_dict = sparql_with_Qid(wikidata_id_usr)
                        note_flag = False
                        modified_fields = ['person_id']
                        if 'gender' in person_dict and row['sex'] != person_dict['gender']:
                            modified_fields.append('sex')
                            note_flag = True
                        if 'birthyear' in person_dict and row['birthyear'] != person_dict['birthyear']:
                            modified_fields.append('birthyear')
                            note_flag = True
                        if 'deathyear' in person_dict and row['deathyear'] != person_dict['deathyear']:
                            modified_fields.append('deathyear')
                            note_flag = True
                        if 'birthplace' in person_dict and row['place_of_birth'] != person_dict['birthplace']:
                            modified_fields.append('place_of_birth')
                            note_flag = True
                        if note_flag:
                            if isinstance(row['note'], str):
                                row['note'] = row['note'] + ' Fields "' + ", ".join(
                                    modified_fields) + '" in this table is/are updated.  By SemBot.'
                            else:
                                row['note'] = 'Fields "' + ", ".join(modified_fields) + '" is/are updated.  By SemBot.'
                            print('For row', index, ' :', "Warning: you should input at least a person_id even if "
                                                          "there is no matched wikidata_id. By SemBot.")
                            # Todo Log: warning
                            return row, last_person_id
                        else:
                            if isinstance(row['note'], str):
                                row['note'] = row['note'] + ' Field `person_id` in this table is updated.  By SemBot.'
                            else:
                                row['note'] = 'Field `person_id` in this table is updated.  By SemBot.'
                            print('For row', index, ' :', "Warning: You should look the person in row ", index,
                                  " up in Wikidata and input the `wikidata_id` in your table in the future. By SemBot.")
                            # Todo Log: Warning
                            return row, last_person_id
                else:
                    last_person_id = row['person_id']
                    if isinstance(row['note'], str):
                        row['note'] = row['note'] + ' Field `person_id` is updated. No match in Wikidata.  By SemBot.'
                    else:
                        row['note'] = 'Field `person_id` is updated. No match in Wikidata.  By SemBot.'
                    # Todo log: info:
                    return row, last_person_id


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Validate CSV columns and auto fill information for Person')
    parser.add_argument('person_csv', type=str, help="Path of the CSV file to be autofilled")
    parser.add_argument('--update', help='Iteration through CSV and update it')
    parser.add_argument('--version', action='version', version='version 1.0.0', help="print version")
    parser.add_argument('-v', '--verbose',
                    action='count',
                    dest='verbosity',
                    default=0,
                    help="verbose output (repeat for increased verbosity)")
    parser.add_argument('-q', '--quiet',
                    action='store_const',
                    const=-1,
                    default=0,
                    dest='verbosity',
                    help="quiet output (show errors only)")

    args = parser.parse_args()

    # #################################################################
    # # 1. Check the input Person.csv
    # #################################################################
    # if not args.person_csv.endswith('Person.csv'):
    #     print('File invalid. You should use only Person.csv as the first argument\n')
    # else:
    #     print("--> Validate 1/2 \nPerson.csv is going to be checked.\n")
    #
    # df = pd.read_csv(args.person_csv, index_col=0)
    validate_result, df = validate('../CSV/Person.csv')
    if not validate_result:
        print('Error: Please check your Person.csv and re-run this tool. By SemBot.')
        quit()
    print('\n======= Finished Checking ========')

    #################################################################
    # 2. Update Person.csv
    #################################################################
    # The following 3 lines should be activaed once this script is done
    # df_person_Github = pd.read_csv(PERSON_CSV_GITHUB)
    # with open('../CSV/df_person_Github.csv', 'w') as f:
    #     f.write(df_person_Github.to_csv())
    df_person_gh = pd.read_csv('../CSV/df_person_Github_fake.csv')  # unofficial version
    check_gh(df_person_gh)
    last_person_id, person_ids_gh, wikidata_ids_GH = get_last_id(df_person_gh)
    for index, row in df.iterrows():
        print('-------------\nFor row ', index, ' :')
        print(row.tolist())
        row, last_person_id = check_each_row(index, row, df_person_gh, person_ids_gh, last_person_id, wikidata_ids_GH)
    with open('../CSV/Person_updated_V2.csv', 'w') as f:
        f.write(df.to_csv())

    # Todo: the condition of updating 'last_modified', 'last_modified_by'

    # here we must check if wikidata is already existed after the checking of wikipedia link
    # if len(row['wikipedia_link']) < 1:
    #     for index_GitHub, row_GitHub in df_person_Github.iterrows():
    #         if row_GitHub['person_id'] == row['person_id']:
    #             if isinstance(row_GitHub['source_1'], str) and ".wikipedia.org/wiki/" in row_GitHub['source_1']:
    #                 wikipdia_link = row_GitHub['source_1']
    #             elif isinstance(row_GitHub['source_2'], str) and ".wikipedia.org/wiki/" in row_GitHub['source_2']:
    #                 wikipdia_link = row_GitHub['source_2']
    #             else:
    #                 wikipdia_link = ''
    #
    #             if wikipdia_link is not None:
    #                 wikidata_id = get_Qid_from_wikipedia_url(row_GitHub)
    #             else:
    #                 wikidata_id = ''

    #
    # # And then check if wikipedia_link field is empty or not:
    # for index_GitHub, row_GitHub in df_person_Github.iterrows():
    #     if row_GitHub['person_id'] == row['person_id']:
    #         if isinstance(row_GitHub['source_1'], str) and ".wikipedia.org/wiki/" in row_GitHub['source_1']:
    #             wikipdia_link = row_GitHub['source_1']
    #         elif isinstance(row_GitHub['source_2'], str) and ".wikipedia.org/wiki/" in row_GitHub['source_2']:
    #             wikipdia_link = row_GitHub['source_2']
    #         else:
    #             wikipdia_link = ''
    #
    #         if wikipdia_link is not None:
    #             wikidata_id = get_Qid_from_wikipedia_url(row_GitHub)
    #         else:
    #             wikidata_id = ''
    #             df.loc[index] = [row['person_id'], row_GitHub['family_name'], row_GitHub['first_name'],row_GitHub[
    #                              'name_lang'], row_GitHub['sex'], row_GitHub['birthyear'],row_GitHub['deathyear'],
    #                              row_GitHub['place_of_birth'], wikipdia_link, wikidata_id, row_GitHub['created'],
    #                              row_GitHub['created_by'], time.strftime("%Y-%m-%d", time.localtime()),'SemBot']

    #     dict = sparql_with_Qid(row['wikidata_id'])
    #     df = update(df, dict)
    #     # Here, in the future, can check if name returned by SPARQL in a list of family_name, first_name
    #     # combinations.
    # elif len(row['wikipedia_link']) > 0:
    #     link = row['wikipedia_link']
    #     if len(link) > 30:
    #         language = link[8:10]
    #         name = link[30:]
    #         # Use MediaWiki API to query
    #         link = "https://" + language + ".wikipedia.org/w/api.php?action=query&prop=pageprops&titles=" + \
    #                name + "&format=json"
    #         response = requests.get(link).json()
    #         if 'pageprops' in list(response['query']['pages'].values())[0]:
    #             pageprops = list(response['query']['pages'].values())[0]['pageprops']
    #         if 'wikibase_item' in pageprops:
    #             Qid = list(response['query']['pages'].values())[0]['pageprops']['wikibase_item']
    #     df.at[index, 'wikidata_id'] = Qid
    #     dict = sparql_with_Qid(Qid)
    #     df = update(df, dict)
    #     # Here, in the future, can check if name returned by SPARQL in a list of family_name, first_name
    #     # combinations.

    #         else:
    #             name_ordered = order_name_by_language(row)
    #             person = sparql_by_name(name_ordered, row['name_lang'], 2)
    #             if len(person) == 0:
    #                 print("There is no match for this person entry with row index ", index)
    #                 pass
    #             else:
    #                 weight = 0
    #                 weights = []
    #                 wiki = []
    #                 for Q_id, p in person.items():
    #                     # all the matched fields will add weight 1 to the total weight for this Q_id
    #                     if 'gender' in p:
    #                         if p['gender'] == row['sex']:
    #                             weight += 1
    #                     elif 'birthyear' in p:
    #                         if p['birthyear'] in row['birthyear']:
    #                             weight += 1
    #                     elif 'deathyear' in p:
    #                         if p['deathyear'] in row['deathyear']:
    #                             weight += 1
    #                     elif 'place_of_birth' in p:
    #                         if p['place_of_birth'] == row['place_of_birth']:
    #                             weight += 1
    #                     weights.append(weight)
    #                     wiki.append(p)
    #                     weight = 0
    #                 if len(wiki) > 0:
    #                     max_value = max(weights)
    #                     max_index = weights.index(max_value)  # return the first match
    #                     if max_value == 0:
    #                         dict = wiki[0]
    #                     else:
    #                         dict = wiki[max_index]
    #                     df = update(df, dict)
    #                     df['note'] = "uncertain match"
    #                 else:
    #                     print('There is no match for row with index ', index)
    #                     print('Here is the information contained in this row: \n', row)
    #
    # # For statistics:
    # updated_rows_sum = df['last_modified_by'].value_counts().SemBot
    # rows_sum = len(df.index)
    # print(
    #     "==================================\n==========  Update      "
    #     "==========:\n==================================\nFinished "
    #     "Updating\n\n")
    # print("==================================\n==========  Statistics  "
    #       "==========\n==================================\nAmong all the ", rows_sum, " rows, you have ",
    #       updated_rows_sum, " rows updated\n\n")
    #
    # with open('../CSV/Person_updated.csv', 'w') as f:
    #     f.write(df.to_csv())
