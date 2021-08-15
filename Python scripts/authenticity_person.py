"""
This is a python script to check authenticity of Named Entities in /Readact/csv/data and in SCB.
Main idea:
- Read ReadAct CSV files, get lookups
- Get the Q-identifier with library wikibaseintegrator for each lookup
- Use SPARQL to retrieve the property we need
- Compare wikidata item properties with data in ReadAct
"""
import pandas as pd
import requests
import time
from authenticity_space import read_space_csv

URL  = "https://query.wikidata.org/sparql"

QUERY = """
SELECT ?person ?personLabel ?ybirth ?ydeath ?birthplaceLabel ?genderLabel
WHERE {{ 
{{?person wdt:P31 wd:Q5 ;
        rdfs:label "{}"@{} . }} UNION {{?person wdt:P31 wd:Q5 ;
        skos:altLabel "{}"@{} . }}
OPTIONAL {{ ?person  wdt:P569  ?birth . BIND(year(?birth) as ?ybirth) }}
OPTIONAL {{ ?person  wdt:P570  ?death . BIND(year(?death) as ?ydeath) }}
OPTIONAL {{ ?person wdt:P19  ?birthplace . }}
OPTIONAL {{ ?person  wdt:P21  ?gender . }}
OPTIONAL {{ ?person skos:altLabel ?altLabel . }}

SERVICE wikibase:label {{ bd:serviceParam wikibase:language  "[AUTO_LANGUAGE], en"}}
}}
GROUP BY ?person ?personLabel ?ybirth ?ydeath ?birthplaceLabel ?genderLabel
LIMIT 250
"""


def read_person_csv(person_url="https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Person.csv"):
    """
    A function to read "Person.csv".
    :param filename: "Person.csv".
    :return: a dictionary: key: unique person_id; value: [family_name,first_name,name_lang,sex,birthyear,deathyear]
    "name_lang" is used to decide if white space needs to be added into name or not.
    """
    df = pd.read_csv(person_url, error_bad_lines=False)
    print(df)
    person_dict = {}
    place_dict = read_space_csv()
    for index, row in df.iterrows():
        key = (row['person_id'], row['name_lang'])
        if key not in person_dict:
            if row['person_dict'] in place_dict:
                person_dict[key] = [row['family_name'], row['first_name'], row['sex'], row['birthyear'],
                                    row['deathyear'], row['alt_name'], place_dict[str(row['place_of_birth'])][0]]
            else:
                person_dict[key] = [row['family_name'], row['first_name'], row['sex'], row['birthyear'],
                                    row['deathyear'], row['alt_name'], row['place_of_birth']]
                print("Please check. A space_id is not in Space.csv.")
        else:
            print("Probably something wrong")
    return person_dict


def compare(person_dict, sleep=2):
    no_match_list = []
    for k, v in person_dict.items():
        lang = k[1]
        if isinstance(v[1], float):
            lookup = v[0]
        else:
            if lang == "en":
                lookup = v[0] + " " + v[1]
            elif lang == "zh":
                lookup = v[0] + v[1]

        person = _sparql(lookup, lang, sleep)
        if len(person) == 0:
            print("No match: ", k, v)
            no_match_list.append((k, v))
            continue
        else:
            for p in person:
                if 'birthyear' in p:
                    if p['birthyear'] == v[3]:
                        print("---A match: ", k, v)
                        continue
                elif 'deathyear' in p:
                    if p['deathyear'] == v[4]:
                        print("---A match: ", k, v)
                        continue
                elif 'gender' in p:
                    if p['gender'] == v[2]:
                        print("---A match: ", k, v)
                        continue
                else:
                    no_match_list.append((k, v))
                    print("No match: ", k, v)
    return no_match_list


def _sparql(lookup, lang, sleep=2):
    if len(lookup) == 0:
        return None
    person = []
    with requests.Session() as s:
        response = s.get(URL, params={"format": "json", "query": QUERY.format(lookup, lang, lookup, lang)})
        if response.status_code == 200:  # a successful response
            results = response.json().get("results", {}).get("bindings")
            if len(results) == 0:
                print("-----Didn't find:", lookup)
            else:
                # print(results)
                for r in results:
                    person_wiki = {}
                    if "ybirth" in r:
                        person_wiki["birthyear"] = r['ybirth']['value']
                    if "ydeath" in r:
                        person_wiki["deathyear"] = r['ydeath']['value']
                    if "genderLabel" in r:
                        person_wiki["gender"] = r['genderLabel']['value']
                    if "birthplaceLabel" in r:
                        person_wiki['birthplace'] = r['birthplaceLabel']['value']
                    person.append(person_wiki)
        time.sleep(sleep)
    return person


if __name__ == "__main__":
    person_dict = read_person_csv("https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Person.csv")

    print(person_dict)

    sample_dict = {('AG0623', 'en'): ['Chen', 'Yiyang', 'male', '1947', 'XXXX', 'Nah', 'SP0073'], ('AG0623','zh'): ['陈', '一阳', 'male', '1947', 'XXXX', 'NaN', 'SP0073'], ('AG0624', 'en'): ['Wang', 'Xizhe', 'male', '1948', 'XXXX', 'NaN', 'SP0009'], ('AG0624', 'zh'): ['王', '希哲', 'male', '1948', 'XXXX', 'NaN', 'SP0009'], ('AG0625', 'en'): ['Guo', 'Hongzhi', 'male', '1929', '1998', 'NaN', 'SP0433'], ('AG0625', 'zh'): ['郭', '鸿志', 'male', '1929', '1998', 'NaN', 'SP0433'], ('AG0626', 'en'): ['anonymous', 'NaN', 'unknown', 'XXXX', 'XXXX', 'NaN', 'SP0436'], ('AG0626', 'zh'): ['无名', 'NaN', 'unknown', 'XXXX', 'XXXX', 'NaN', 'SP0436'], ('AG1000', 'en'): ['Room', 'Adrian', 'male', '1933', '2010', 'NaN', 'SP1001']}

    no_match_list = compare(person_dict, 2)
    print(no_match_list)
    print("-------length of the no_match_list", len(no_match_list))

    # person = _sparql("Lu Xun", 'en')
    # print(person)





