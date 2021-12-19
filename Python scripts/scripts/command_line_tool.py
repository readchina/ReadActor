import argparse
import time
import pandas as pd
from authenticity_person import _sparql


data_dictionary_github = "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data_dictionary.csv"
person_csv_github = "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Person.csv"


def update_CSV(arg_file, required_columns):
    """
    A function to update CSV table if anything changed. If not, return none.
    :param required_columns:
    :param arg_file: the file input by user
    :return: updated dataframe or None
    """
    if __validate_person(arg_file, required_columns):
        output = ''
        return output
    else:
        return None


def __validate_person(arg_file, required_columns):
    """
    Three conditions to make the input CSV to be valid:
    1. The name is Person.csv
    2. Includes necessary 12 columns: 'person_id', 'family_name', 'first_name', 'name_lang', 'sex',
       'birthyear', 'deathyear', 'place_of_birth', 'created', 'created_by', 'last_modified',
       'last_modified_by'
    3. All the column names are valid according to the data_dictionay.csv on GitHub.
    :param arg_file: the file input by user
    :return: True or False
    """
    # For condition 1:
    if arg_file != 'Person.csv':
        return False

    df_user_input = pd.read_csv(arg_file, error_bad_lines=False)
    if not set(required_columns).issubset(df_user_input.columns.tolist()):
        return False


    # df_data_dictionary = pd.read_csv(data_dictionary_github, error_bad_lines=False)
    # df_Person_GitHub = pd.read_csv(person_csv_github, error_bad_lines=False)
    # print(df_Person_GitHub.columns)
    # last_person_id = df_Person_GitHub['person_id'].iloc[-1]
    # print(last_person_id)


if __name__ == "__main__":
    required_columns = ['person_id', 'family_name', 'first_name', 'name_lang', 'sex',
       'birthyear', 'deathyear', 'place_of_birth', 'wikipedia_link', 'wikidata_id', 'created', 'created_by',
                        'last_modified', 'last_modified_by'] # 'wikipedia_link','wikidata_id' are waiting for discussion


    # parser = argparse.ArgumentParser()
    # parser.add_argument('--file', type=str, help="path of the CSV file to be autofilled")
    # args = parser.parse_args()
    # with open(args.file, 'r') as f:
    #     output = update_CSV(f, required_columns)
    #
    # if output is not None:
    #     with open('Person.csv', 'w') as f:
    #         pass






    # parser = argparse.ArgumentParser(description='An auto-fill tool for given person name and Wikipedia link.')
    # parser.add_argument('-i', '--input', dest='input_filename', action='store', default='data.csv', type=str, help='specify the input file, default `data.csv`')
    # parser.add_argument('-o', '--output', dest='output_filename', action='store',default='outliers_result.csv', type=str, help='specify the output file, default `outliers_result.csv`')
