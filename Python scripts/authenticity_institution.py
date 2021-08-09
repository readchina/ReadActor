"""
This is a python script to check authenticity of Named Entities in /Readact/csv/data and in SCB.
Main idea:
- Read ReadAct CSV files, get lookups
- Get the Q-identifier with library wikibaseintegrator for each lookup
- Use SPARQL to retrieve the property we need
- Compare wikidata item properties with data in ReadAct
"""
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
import time
from wikibaseintegrator import wbi_core


def read_institution_csv(inst_url):
    """
    A function to read "Person.csv".
    :param filename: "Person.csv".
    :return: a dictionary: key: unique person_id; value: [family_name,first_name,name_lang,sex,birthyear,deathyear]
    "name_lang" is used to decide if white space needs to be added into name or not.
    """
    df = pd.read_csv(inst_url, error_bad_lines=False)
    print(df)
    inst_dict = {}
    for index, row in df.iterrows():
        key = (row[0], row[2])
        if key not in inst_dict:
            # key: string:  (inst_id, inst_name_lang)
            # value: list: [inst_name,place,start,end]
            inst_dict[key] = [row[1], row[3], row[4], row[5]]
            # print("inst_dict", inst_dict)
        else:
            print("Probably something wrong")
    return inst_dict




def _get_q_ids(lookup=None):
    """
    A function to search qnames in wikidata with a lookup string.
    :param lookup: a string
    :return: a list of item identifiers (first 10)
    """
    e = wbi_core.FunctionsEngine()
    instance = e.get_search_results(search_string=lookup,
                                    search_type='item')

    if len(instance) > 0:
        return instance[0:1]
    else:
        print("Lookup '", lookup, "' not in Wikidata. Didn't find Q-ids.")
        return None


if __name__ == "__main__":
    inst_dict = read_institution_csv("https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data"
                                       "/Institution.csv")
    print(inst_dict)

    no_match_list = compare(inst_dict, 20)
    print("-------length of the no_match_list", len(no_match_list))





