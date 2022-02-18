import argparse
import time
import pandas as pd

from authenticity_person import order_name_by_language, get_Qid_from_wikipedia_url, sparql_with_Qid, sparql_by_name

DATA_DICTIONARY_GITHUG = "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data_dictionary.csv"
PERSON_CSV_GITHUB = "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Person.csv"
EXPECTED_COL = ['person_id', 'family_name', 'first_name', 'name_lang', 'sex', 'birthyear', 'deathyear',
                        'place_of_birth', 'wikipedia_link', 'wikidata_id', 'created', 'created_by',
                        'last_modified', 'last_modified_by', 'note']
MINIMAL_COL = ['family_name', 'first_name', 'name_lang']


def update(df, dict):
    flag = False
    if 'gender' in dict:
        df.at[index, 'sex'] = dict['gender']
        flag = True
    if 'birthyear' in dict:
        df.at[index, 'birthyear'] = dict['birthyear']
        flag = True
    if 'deathyear' in dict:
        df.at[index, 'deathyear'] = dict['deathyear']
        flag = True
    if 'place_of_birth' in dict:
        df.at[index, 'place_of_birth'] = dict['place_of_birth']
        flag = True
    if flag:
        df.at[index, 'last_modified'] = time.strftime("%Y-%m-%d", time.localtime())
        df.at[index, 'last_modified_by'] = 'SemBot'
    return df


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


def __compare_two_rows(row, row_GH):
    """
    This function will be triggered when `person_id` and `wikidata_id` are the same.
    It will compare the rest fields of two rows from two dataframes seperately.

    :param row: one row from the user-uploaded Person.csv
    :param row_GH: the row in Person.csv on GitHub which has the same `person_id` and `wikidata_id` with the other
    parameter
    :return: True if all are matching, otherwise False
    """
    fields_to_be_compared = ['family_name', 'first_name', 'name_lang', 'sex', 'birthyear', 'deathyear',
                        'place_of_birth', 'wikipedia_link', 'created', 'created_by',
                        'last_modified', 'last_modified_by', 'note']
    for i in fields_to_be_compared:
        if row[i] != row_GH[i]:
            return False
    return True


def __overwrite(row, row_GH):
    """
    This function will overwrite all the fields except `person_id` and `wikidata_id`.
    :param row: one row from the user-uploaded Person.csv
    :param row_GH: the row in Person.csv on GitHub which has the same `person_id` and `wikidata_id` with the other
    parameter
    :return: row which is modified
    """
    fields_to_be_overwritten = ['family_name', 'first_name', 'name_lang', 'sex', 'birthyear', 'deathyear',
                        'place_of_birth', 'wikipedia_link', 'created', 'created_by',
                        'last_modified', 'last_modified_by', 'note']
    for i in fields_to_be_overwritten:
        if row[i] != row_GH[i]:
            row[i] = row_GH[i]
    if isinstance(row['note'], float):
        print(row['note'])
        row['note'] = 'SemBot'
    elif isinstance(row['note'], str):
        row['note'] = row['note'].append(', SemBot')
    else:
        row['note'] = 'SemBot'
    return row



def compare_wikidata_ids(row, wikidata_id_U, df_person_GH):
    if 'wikidata_id' in df_person_GH.columns:
        row_GH_index = df_person_GH.index[(df_person_GH['person_id'] == row['person_id']) & (
                df_person_GH['name_lang'] == row['name_lang'])].tolist()[0]
        row_GH = df_person_GH.iloc[row_GH_index]
        wikidata_id_GH = row_GH['wikidata_id']

        if wikidata_id_GH == wikidata_id_U:
            res = __compare_two_rows(row, row_GH)
            if not res:
                row = __overwrite(row, row_GH)
                return row
            else:
                return row
        else: # `person_id`s match but `wikidata_id`s are not matching
            row['note'] = 'Error: `wikidata_id` is not matching with GitHub data. Please check.'
            print('\n\nError: \nFor row', index, ' :\nError: `wikidata_id` is not matching with '
                                                  'GitHub '
                                            'data. '
                                     'Please '
                                    'check your data')
            return row
    else: # `person_id` on GitHub but `wikidata_id` columns does not exist on GitHub, which actually
# should not be this case
        print('There is no `wikidata_id` column in the Person.csv on GitHub. Please inform someone to '
              'check it.')
        exit()


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
        print('Please check your Person.csv and re-run this tool.')
        quit()
    print('\n======= Finished Checking ========')


    #################################################################
    # 2. Update Person.csv
    #################################################################
    # The following 3 lines should be activaed once this script is done
    # df_person_Github = pd.read_csv(PERSON_CSV_GITHUB)
    # with open('../CSV/df_person_Github.csv', 'w') as f:
    #     f.write(df_person_Github.to_csv())
    df_person_GH = pd.read_csv('../CSV/df_person_Github_fake.csv')  # For the official version, the "_fake" should be
    # removed
    # Get the person_id list from GitHub
    person_ids_GH = df_person_GH['person_id'].tolist()
    person_ids_GH.sort()
    # Get the current last person_id
    last_id_in_GH = person_ids_GH[-1]

    for index, row in df.iterrows():
        # if `note` field contains "Skip", skip this line, do nothing to this line
        if row['note'] == 'Skip':
            continue
        # If `person_id` in GitHub database already:
        # If yes, check if `wikidata_id` in GitHub database already
        # If not, use `family_name`, `first_name`, and `name_lang` to query for `wikidata_id`
        else:
            if row['person_id'] in person_ids_GH:
                #`person_id` on GitHub, `wikidata_id` is in this user inputted row
                if isinstance(row['wikidata_id'], str):
                    wikidata_id_U = row['wikidata_id']
                    # The same `person_id` on GitHub, there is an `wikidata_id` field on GitHub
                    row = compare_wikidata_ids(row, wikidata_id_U, df_person_GH)

                # `person_id` on GitHub, but `wikidata_id` not in this user inputted row
                # Should use `family_name`, `first_name`, `name_lang`, to query for `wikidata_id`.
                else:
                    names = order_name_by_language(row)
                    person = sparql_by_name(names, row['name_lang'], 2)
                    print('@@@@@@@@@@@@')
                    for key in person.keys():

                        wikidata_id_U = key
                        break # Here, for now, we always only want the first one
                        # To Do: Need to check the order again
                    row = compare_wikidata_ids(row, wikidata_id_U, df_person_GH)



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


            else:
                pass

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
