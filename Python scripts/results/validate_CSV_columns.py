import pandas as pd

path_data_dictionary = "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data_dictionary.csv"

def validate_csv(csv_path):
    df = pd.read_csv(csv_path, error_bad_lines=False)
    df_data_dictionary = pd.read_csv(path_data_dictionary, error_bad_lines=False)
    for item in df.columns:
        if item not in df_data_dictionary['column'].unique():
            print('-----')
            print(item, ' is not in the column "column" of the data_dictionary.csv table.')


if __name__ == "__main__":
    person_csv = 'https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Person.csv'
    validate_csv(person_csv)

    """
    Outout with the curret Person.csv:
    
    -----
    page_1  is not in the column "column" of the data_dictionary.csv table.
    -----
    page_2  is not in the column "column" of the data_dictionary.csv table.
    """
