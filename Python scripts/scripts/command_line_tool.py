import argparse
import sys
import time
import pandas as pd
from authenticity_person import get_Qid_from_wikipedia_url, sparql_with_Qid

data_dictionary_github = "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data_dictionary.csv"
person_csv_github = "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Person.csv"


def update_CSV(arg_file):
    """
    A function to update CSV table if anything changed. If not, return none.
    :param required_columns:
    :param arg_file: the file input by user
    :return: updated dataframe or None
    """
    pass


if __name__ == "__main__":
    required_columns = ['person_id', 'family_name', 'first_name', 'name_lang', 'sex',
       'birthyear', 'deathyear', 'place_of_birth', 'wikipedia_link', 'wikidata_id', 'created', 'created_by',
                        'last_modified', 'last_modified_by'] # 'wikipedia_link','wikidata_id' are waiting for discussion


    parser = argparse.ArgumentParser(description='Validate CSV columns and auto fill information for Person')
    parser.add_argument('person_csv', type=str, help="Path of the CSV file to be autofilled")
    parser.add_argument('--update', help='Iteration through CSV and update it')
    parser.add_argument('--version', action='version', version='version 1.0.0')

    args = parser.parse_args()

    if not args.person_csv.endswith('Person.csv'):
        print('File invalid. You should use only Person.csv as the first argument')
    else:
        print("========== Validate 1/2 ==========:\nPerson.csv is going to be checked.")

    user_file = pd.read_csv(args.person_csv, index_col=0)
    if not set(required_columns).issubset(user_file.columns.tolist()):
        print('There must be 14 mandatory columns in CSV table. Please check your file')
    else:
        print("========== Validate 2/2 ==========:\nAll 14 mandatory columns are included. Well done!")


    
