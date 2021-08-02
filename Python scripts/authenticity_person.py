"""
This is a python script to check authenticity of Named Entities in /Readact/csv/data and in SCB.
Main idea:
- Read ReadAct CSV files, get lookups
- Get the Q-identifier with library wikibaseintegrator for each lookup
- Use SPARQL to retrieve the property we need
- Compare wikidata item properties with data in ReadAct
"""
import json

import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON
import requests
import time
from wikibaseintegrator import wbi_core
from wikidataintegrator import wdi_core


def read_person_csv(person_url):
    """
    A function to read "Person.csv".
    :param filename: "Person.csv".
    :return: a dictionary: key: unique person_id; value: [family_name,first_name,name_lang,sex,birthyear,deathyear]
    "name_lang" is used to decide if white space needs to be added into name or not.
    """
    df = pd.read_csv(person_url, error_bad_lines=False)
    print(df)
    geo_code_dict = {}
    person_dict = {}
    for index, row in df.iterrows():
        key = (row[0], row[3])
        if key not in person_dict:
            # key: string:  (person_id, name_lang)
            # value: list: [family_name,first_name,sex,birthyear,deathyear]
            person_dict[key] = [row[1], row[2], row[4], row[6], row[7]]
            print("person_dict", person_dict)
        else:
            print("Probably something wrong")
    return person_dict


def compare(person_dict):
    no_match_list = []
    for k, v in person_dict.items():
        print("==============")
        print("key: ", k)
        print("value: ", v)
        if k[1] != "zh":
            print(type(v[0]))
            print(type(v[1]))
            name = v[0] + " " + v[1]
        else:
            name = v[0] + v[1]
        print("name: ", name)
        q_ids = _get_q_ids(name)
        if q_ids is None:
            no_match_list.append((k,v))
            continue
        person_wiki_dict = _sparql(q_ids)
        if not person_wiki_dict:
            no_match_list.append((k,v))
            continue
        if 'gender' in person_wiki_dict:
            if person_wiki_dict['gender'] != v[2]:
                no_match_list.append((k,v))
                continue
        if 'birthyear' in person_wiki_dict:
            if person_wiki_dict['birthyear'] != v[3]:
                no_match_list.append((k,v))
                continue
        if 'deathyear' in person_wiki_dict:
            if person_wiki_dict['deathyear'] != v[4]:
                no_match_list.append((k,v))
                continue
    return no_match_list


def _sparql(q_ids):
    if q_ids is None:
        return []
    person_wiki_dict = {}
    for index, q in enumerate(q_ids):
        # Todo: This query has problems.
        # Can try birthyear
        # date of birth (P569)
        # date of death (P570)
        # sex or gender (P21)
        # male (Q6581097)
        # female (Q6581072)
        queryString = """
        PREFIX  schema: <http://schema.org/>
        PREFIX  bd:   <http://www.bigdata.com/rdf#>
        PREFIX  wdt:  <http://www.wikidata.org/prop/direct/>
        PREFIX  wikibase: <http://wikiba.se/ontology#>
        
        SELECT DISTINCT  ?item ?itemLabel (SAMPLE(?date_of_birth) AS ?date_of_birth) (SAMPLE(?date_of_death) AS 
        ?date_of_death) 
        (SAMPLE(?gender) AS ?gender) 
        WHERE
          { ?article  schema:about       ?item ;
                      schema:inLanguage  "en" ;
                      schema:isPartOf    <https://en.wikipedia.org/>
            FILTER ( ?item = <http://www.wikidata.org/entity/""" + q + """> )
            OPTIONAL
              { ?item  wdt:P569  ?date_of_birth }
            OPTIONAL
              { ?item  wdt:P570  ?date_of_death }
            OPTIONAL
              { ?item  wdt:P21  ?gender }
            SERVICE wikibase:label
              { bd:serviceParam wikibase:language  "en"
              }
          }
        GROUP BY ?item ?itemLabel 
        """
        sparql = SPARQLWrapper("https://query.wikidata.org/sparql")

        sparql.setQuery(queryString)

        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        print(results)
        if results['results']['bindings']:
            if "date_of_birth" in results['results']['bindings'][0]:
                birthyear = results['results']['bindings'][0]['date_of_birth']['value'][0:4]
                print(birthyear)
                person_wiki_dict["birthyear"] = birthyear
            if "date_of_death" in results['results']['bindings'][0]:
                deathyear = results['results']['bindings'][0]['date_of_death']['value'][0:4]
                print(deathyear)
                person_wiki_dict["deathyear"] = deathyear
            if "gender" in results['results']['bindings'][0]:
                gender = results['results']['bindings'][0]['gender']['value']
                if gender == "http://www.wikidata.org/entity/Q6581097":
                    gender = "male"
                if gender == "http://www.wikidata.org/entity/Q6581072":
                    gender = "female"
                print(gender)
                person_wiki_dict["gender"] = gender
        return person_wiki_dict


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
        print("Lookup not in database")
        return None


if __name__ == "__main__":
    # person_dict = read_person_csv("https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Person.csv")

    person_dict = {('AG0001', 'en'): ['Lu', 'Xun', 'male', '1881', '1936'], ('AG0001', 'zh'): ['鲁', '迅', 'male', '1881', '1936'], ('AG0002', 'en'): ['Mou', 'Dunbai', 'male', '1947', 'XXXX'], ('AG0002', 'zh'): ['牟', '敦白', 'male', '1947', 'XXXX'], ('AG0003', 'en'): ['Zhang', 'Langlang', 'male', '1943', 'XXXX'], ('AG0003', 'zh'): ['张', '朗朗', 'male', '1943', 'XXXX'], ('AG0004', 'en'): ['Mang', 'Ke', 'male', '1950', 'XXXX'], ('AG0004', 'zh'): ['芒', '克', 'male', '1950', 'XXXX'], ('AG0005', 'en'): ['Peng', 'Gang', 'male', '1952', 'XXXX'], ('AG0005', 'zh'): ['彭', '刚', 'male', '1952', 'XXXX'], ('AG0006', 'en'): ['Tan', 'Xiaochun', 'unknown', 'XXXX', 'XXXX'], ('AG0006', 'zh'): ['谭', '晓春', 'unknown', 'XXXX', 'XXXX'], ('AG0007', 'en'): ['Cheng', 'Chuan', 'unknown', 'XXXX', 'XXXX'], ('AG0007', 'zh'): ['呈', '川', 'unknown', 'XXXX', 'XXXX'], ('AG0008', 'en'): ['Lu', 'Zhongnan', 'male', '1950', 'XXXX'], ('AG0008', 'zh'): ['卢', '中南', 'male', '1950', 'XXXX'], ('AG0009', 'en'): ['Bei', 'Dao', 'male', '1949', 'XXXX'], ('AG0009', 'zh'): ['北', '岛', 'male', '1949', 'XXXX'], ('AG0010', 'en'): ['Duo', 'Duo', 'male', '1951', 'XXXX'], ('AG0010', 'zh'): ['多', '多', 'male', '1951', 'XXXX'], ('AG0011', 'en'): ['Xu', 'Haoyuan', 'female', '1949', 'XXXX'], ('AG0011', 'zh'): ['徐', '浩渊', 'female', '1949', 'XXXX']}


    no_match_list = compare(person_dict)
    print("-------no_match_list", no_match_list)

    # sample_dict = {('AG0619', 'en'): ['Qu', 'Yuan', 'male', '-0340', '-0278'], ('AG0619', 'zh'): ['屈', '原', 'male', '-0340', '-0278'], ('AG0620', 'en'): ['Qu', 'Qiubai', 'male', '1899', '1935'], ('AG0620', 'zh'): ['瞿', '秋白', 'male', '1899', '1935']}
    # print("sample_dict: ", sample_dict)
    # q_ids = compare(sample_dict)
    # # print(q_ids)
    # q_ids = ['Q45581047']
    # _sparql(q_ids)




