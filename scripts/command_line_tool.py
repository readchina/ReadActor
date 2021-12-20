import argparse
import time
import pandas as pd
import requests

from authenticity_person import order_name_by_language, get_Qid_from_wikipedia_url, sparql_with_Qid, sparql_by_name

data_dictionary_github = "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data_dictionary.csv"
person_csv_github = "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Person.csv"

if __name__ == "__main__":
    required_columns = ['person_id', 'family_name', 'first_name', 'name_lang', 'sex',
                        'birthyear', 'deathyear', 'birthplace', 'wikipedia_link', 'wikidata_id', 'created',
                        'created_by',
                        'last_modified',
                        'last_modified_by']  # 'wikipedia_link','wikidata_id' are waiting for discussion

    parser = argparse.ArgumentParser(description='Validate CSV columns and auto fill information for Person')
    parser.add_argument('person_csv', type=str, help="Path of the CSV file to be autofilled")
    parser.add_argument('--update', help='Iteration through CSV and update it')
    parser.add_argument('--version', action='version', version='version 1.0.0')

    args = parser.parse_args()

    #################################################################
    # 1. Check the input Person.csv
    #################################################################
    if not args.person_csv.endswith('Person.csv'):
        print('File invalid. You should use only Person.csv as the first argument\n')
    else:
        print("========== Validate 1/2 ==========:\nPerson.csv is going to be checked.\n")

    df = pd.read_csv(args.person_csv, index_col=0)
    df = df.fillna('') # Replace all the nan into empty string
    # print(df)
    if not set(required_columns).issubset(df.columns.tolist()):
        print('There must be 14 mandatory columns in CSV table. Please check your file\n')
    else:
        print("========== Validate 2/2 ==========:\nAll 14 mandatory columns are included. Well done!\n")

    # In the future, should also check if family_name, first_name, name_lang are empty.
    # This three are mandatory.

    #################################################################
    # 2. Update Person.csv
    #################################################################

    df_person_Github = pd.read_csv(person_csv_github)
    person_ids_GitHub = df_person_Github['person_id'].tolist()
    for index, row in df.iterrows():
        if row['person_id'] in person_ids_GitHub:
            # # Choice 1:
            # df.drop(index, inplace=True)
            # df.reset_index(drop=True, inplace=True)
            # print("The entry with 'person_id' ", row['person_id'], " and name ", row['first_name'] + " " + row[
            #     'family_name'], " exists already in ReadAct. ")

            # Choice 2:
            for index_GitHub, row_GitHub in df_person_Github.iterrows():
                if row_GitHub['person_id'] == row['person_id']:
                    if isinstance(row_GitHub['source_1'], str) and ".wikipedia.org/wiki/" in row_GitHub['source_1']:
                        wikipdia_link = row_GitHub['source_1']
                    elif isinstance(row_GitHub['source_2'], str) and ".wikipedia.org/wiki/" in row_GitHub['source_2']:
                        wikipdia_link = row_GitHub['source_2']
                    else:
                        wikipdia_link = ''

                    if wikipdia_link is not None:
                        wikidata_id = get_Qid_from_wikipedia_url(row_GitHub)
                    else:
                        wikidata_id = ''

                    df.loc[index] = [row['person_id'], row_GitHub['family_name'], row_GitHub['first_name'],
                                     row_GitHub[
                        'name_lang'], row_GitHub['sex'], row_GitHub['birthyear'], row_GitHub['deathyear'],
                                     row_GitHub['place_of_birth'], wikipdia_link, wikidata_id, row_GitHub['created'],
                                     row_GitHub['created_by'], time.strftime("%Y-%m-%d", time.localtime()),
                                     'SemBot']

        else:
            if len(row['wikidata_id']) > 0:
                dict = sparql_with_Qid(row['wikidata_id'])
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
                if 'birthplace' in dict:
                    df.at[index, 'birthplace'] = dict['birthplace']
                    flag = True
                if flag == True:
                    df.at[index, 'last_modified'] = time.strftime("%Y-%m-%d", time.localtime())
                    df.at[index, 'last_modified_by'] = 'SemBot'
                # Here, in the future, can check if name returned by SPARQL in a list of family_name, first_name
                # combinations.
            elif len(row['wikipedia_link']) > 0:
                link = row['wikipedia_link']
                if len(link) > 30:
                    language = link[8:10]
                    name = link[30:]
                    # Use MediaWiki API to query
                    link = "https://" + language + ".wikipedia.org/w/api.php?action=query&prop=pageprops&titles=" + \
                           name + "&format=json"
                    response = requests.get(link).json()
                    if 'pageprops' in list(response['query']['pages'].values())[0]:
                        pageprops = list(response['query']['pages'].values())[0]['pageprops']
                    if 'wikibase_item' in pageprops:
                        Qid = list(response['query']['pages'].values())[0]['pageprops']['wikibase_item']
                dict = sparql_with_Qid(Qid)
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
                if 'birthplace' in dict:
                    df.at[index, 'birthplace'] = dict['birthplace']
                    flag = True
                if flag:
                    df.at[index, 'last_modified'] = time.strftime("%Y-%m-%d", time.localtime())
                    df.at[index, 'last_modified_by'] = 'SemBot'
                # Here, in the future, can check if name returned by SPARQL in a list of family_name, first_name
                # combinations.
            else:
                name_ordered = order_name_by_language(row)
                person = sparql_by_name(name_ordered, row['name_lang'], 2)
                # Not finished yet. This section is used when only name is given, no wikipedia link or wikidata id.
                pass

    updated_rows_sum = df['last_modified_by'].value_counts().SemBot
    rows_sum = len(df.index)
    print("========== Update 1/2 ==========:\nFinished Updating\n")
    print("========== Update 2/2 ==========:\nStatistics: You have ", updated_rows_sum, " rows updated, among all the ", rows_sum, " rows\n")
    with open('../CSV/Person_updated.csv', 'w') as f:
        f.write(df.to_csv())

