from authenticity_person import get_Qid_from_wikipedia_url, sparql_with_Qid:
import pandas as pd
import argparse
from validate_CSV_columns import validate_csv

def parse_args():
    parser = argparse.ArgumentParser(description='An auto-fill tool for given person name and Wikipedia link.')
    parser.add_argument('-i', '--input', dest='input_filename', action='store', default='data.csv', type=str, help='specify the input file, default `data.csv`')
    parser.add_argument('-o', '--output', dest='output_filename', action='store',default='outliers_result.csv', type=str, help='specify the output file, default `outliers_result.csv`')








def autofill():



if __name__ == "__main__":
    csv_path = ''



