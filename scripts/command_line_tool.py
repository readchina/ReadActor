import argparse
import time
import pandas as pd

from authenticity_person import order_name_by_language, get_Qid_from_wikipedia_url, sparql_with_Qid, sparql_by_name

DATA_DICTIONARY_GITHUG = "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data_dictionary.csv"
PERSON_CSV_GITHUB = "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Person.csv"
EXPECTED_COL = ['person_id', 'family_name', 'first_name', 'name_lang', 'sex', 'birthyear', 'deathyear',
                'place_of_birth', 'wikidata_id', 'created', 'created_by',
                'last_modified', 'last_modified_by', 'note']
MINIMAL_COL = ['family_name', 'first_name', 'name_lang']

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
                 'place_of_birth', 'wikipedia_link', 'wikidata_id', 'created', 'created_by',
                 'last_modified', 'last_modified_by', 'note']]
        print("All the missing columns are inserted to your csv table now.\nNote that columns outside the 15 expected "
              "columns are dropped.")
        # To Do: rewrite to make sure that each column has a fixed position in any Person.csv
    else:
        df = df[['person_id', 'family_name', 'first_name', 'name_lang', 'sex', 'birthyear', 'deathyear',
                 'place_of_birth', 'wikipedia_link', 'wikidata_id', 'created', 'created_by',
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
    for i in fields_to_be_overwritten:
        if row[i] != row_gh[i]:
            row[i] = row_gh[i]
            note_flag = True
    if note_flag:
        if isinstance(row['note'], str):
            row['note'] = row['note'] + ' Overwritten.  By SemBot.'
        else:
            row['note'] = 'Overwritten.  By SemBot.'
    return row


def __compare_wikidata_ids(index, row, wikidata_id_usr, df_person_GH):
    row_gh_index = df_person_GH.index[(df_person_GH['person_id'] == row['person_id']) & (
            df_person_GH['name_lang'] == row['name_lang'])].tolist()[0]
    row_GH = df_person_GH.iloc[row_gh_index]
    wikidata_id_gh = row_GH['wikidata_id']
    print("wikidata_id_gh: ", wikidata_id_gh)
    print("wikidata_id_usr: ", wikidata_id_usr)
    if wikidata_id_gh == wikidata_id_usr:
        res = __compare_two_rows(row, row_GH)
        if not res:
            return __overwrite(row, row_GH)
    else:  # `person_id`s match but `wikidata_id`s are not matching
        row['note'] = 'Error: `wikidata_id` is not matching with GitHub data. Please check. By SemBot.'
        print('For row', index, ' :', 'Error: `wikidata_id` does not match GitHub data. Please check. By SemBot.')
    return row


def __check_person_id_size(last_id_in_gh):
    if int(last_id_in_gh[2:]) >= 1999:
        print("Warning: It is better to update all person_id in the database. By SemBot.")
        if isinstance(row['note'], str):
            row['note'] = row['note'] + ' Warning: It is better to update all person_id in the database. By SemBot.'
        else:
            row['note'] = 'Warning: It is better to update all person_id in the database. By SemBot.'


def __check_gh(df):  # a function to check if Person.csv on GitHub has `wikidata_id` column and
    # 'wikipedia_link' column
    if 'wikidata_id' not in df.columns:
        print('There is no `wikidata_id` column in the Person.csv on GitHub. Please inform someone to '
              'check it. By SemBot.')
        exit()
    if 'wikipedia_link' not in df.columns:
        print('There is no `wikipedia_link` column in the Person.csv on GitHub. Please inform someone to '
              'check it. By SemBot.')
        exit()


def __get_last_id(df):
    person_ids_GH = df['person_id'].tolist()
    person_ids_GH.sort()
    return person_ids_GH[-1], person_ids_GH


def __check_each_row(index, row, df_person_gh, person_ids_gh, last_person_id):
    if row['note'] == 'Skip':
        return row, last_person_id
    else:
        if row['person_id'] in person_ids_gh:
            if (isinstance(row['wikidata_id'], str) is True) and (len(row['wikidata_id']) > 0):
                wikidata_id_usr = row['wikidata_id']
                return __compare_wikidata_ids(index, row, wikidata_id_usr, df_person_gh), last_person_id
            else:
                names = order_name_by_language(row)
                person = sparql_by_name(names, row['name_lang'], 2)
                if len(person) > 0:
                    wikidata_id_usr = next(iter(person))
                    return __compare_wikidata_ids(index, row, wikidata_id_usr, df_person_gh), last_person_id
                else:
                    print('For row', index, ' :', 'Warning: No match in Wikidata database. By SemBot.')
                    if isinstance(row['note'], str):
                        row['note'] = row['note'] + ' Warning: No match in Wikidata database. By SemBot.'
                    else:
                        row['note'] = 'Warning: No match in Wikidata database. By SemBot.'
                    return row, last_person_id

        # When the given `person_id` is not in GitHub database
        else:
            __check_person_id_size(last_person_id)
            if (isinstance(row['wikidata_id'], str) is True) and (len(row['wikidata_id']) > 0):
                if row['wikidata_id'] in df_person_gh['wikidata_id'].tolist():
                    row_gh_index = df_person_gh.index[(df_person_gh['wikidata_id'] == row['wikidata_id']) & (
                            df_person_gh['name_lang'] == row['name_lang'])].tolist()[0]
                    row_gh = df_person_gh.iloc[row_gh_index]
                    res = __compare_two_rows(row, row_gh)
                    if not res:
                        row = __overwrite(row, row_gh)
                        row['person_id'] = row_gh['person_id']
                        return row, last_person_id
                else:
                    # row['person_id'] = "AG" + str(int(last_person_id[2:]) + 1)
                    # last_person_id = row['person_id']
                    wikidata_id_usr = row['wikidata_id']
                    person_dict = sparql_with_Qid(wikidata_id_usr)
                    if len([person_dict]) > 0:
                        for key in list(person_dict.keys()):
                            if key in ['birthyear', 'deathyear']:
                                row[key] = person_dict[key]
                            if key == 'birthplace':
                                row['place_of_birth'] = person_dict[key]
                            if key == 'gender':
                                row['sex'] = person_dict[key]
                        return row, last_person_id
                    else:
                        print('For row', index, ' :', 'Warning: Wrong wikidata_id. By SemBot.')
                        if isinstance(row['note'], str):
                            row['note'] = row['note'] + ' Warning: Wrong wikidata_id. By SemBot.'
                        else:
                            row['note'] = 'Warning: Wrong wikidata_id. By SemBot.'
                        return row, last_person_id
            else:
                if isinstance(row['person_id'], str) and (len(row['person_id']) > 0):
                    pass
                else:
                    row['person_id'] = "AG" + str(int(last_person_id[2:]) + 1)
                    last_person_id = row['person_id']
                names = order_name_by_language(row)
                person = sparql_by_name(names, row['name_lang'], 2)
                if len(person) == 0:
                    print('For row', index, ' :', "Warning: No match in Wikidata database.")
                    if isinstance(row['note'], str):
                        row['note'] = row['note'] + ' Warning: No match in Wikidata database.'
                    else:
                        row['note'] = 'Warning: No match in Wikidata database.'
                    return row, last_person_id
                else:
                    wikidata_id_usr = next(iter(person))
                    row['wikidata_id'] = wikidata_id_usr
                    person_dict = person[wikidata_id_usr]
                    for key in list(person_dict.keys()):
                        if key in ['birthyear', 'deathyear']:
                            row[key] = person_dict[key]
                        if key == 'birthplace':
                            row['place_of_birth'] = person_dict[key]
                        if key == 'gender':
                            row['sex'] = person_dict[key]
                    return row, last_person_id


if __name__ == "__main__":

    # parser = argparse.ArgumentParser(description='Validate CSV columns and auto fill information for Person')
    # parser.add_argument('person_csv', type=str, help="Path of the CSV file to be autofilled")
    # parser.add_argument('--update', help='Iteration through CSV and update it')
    # parser.add_argument('--version', action='version', version='version 1.0.0')
    #
    # args = parser.parse_args()

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
    __check_gh(df_person_gh)
    last_person_id, person_ids_gh = __get_last_id(df_person_gh)
    for index, row in df.iterrows():
        print('-------------\nFor row ', index, ' :')
        row, last_person_id = __check_each_row(index, row, df_person_gh, person_ids_gh, last_person_id)
    with open('../CSV/Person_updated_V2.csv', 'w') as f:
        f.write(df.to_csv())



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
