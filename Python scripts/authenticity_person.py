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

URL  = "https://query.wikidata.org/sparql"
QUERY_PERSON_NAME = """
SELECT ?person ?personLabel
WHERE {{
  ?person wdt:P31 wd:Q5 ;
          rdfs:label "{}"@{} .
SERVICE wikibase:label
        {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],en". }}
}}
"""

QUERY1 = """
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
            FILTER ( ?item = <http://www.wikidata.org/entity/"""
QUERY2 = """> )
            OPTIONAL
              { ?item  wdt:P569  ?date_of_birth }
            OPTIONAL
              { ?item  wdt:P570  ?date_of_death }
            OPTIONAL
              { ?item  wdt:P21  ?gender }
            SERVICE wikibase:label
              { bd:serviceParam wikibase:language  "[AUTO_LANGUAGE],en"
              }
          }
        GROUP BY ?item ?itemLabel 
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
    for index, row in df.iterrows():
        key = (row[0], row[3])
        if key not in person_dict:
            # key: string:  (person_id, name_lang)
            # value: list: [family_name,first_name,sex,birthyear,deathyear]
            person_dict[key] = [row[1], row[2], row[4], row[6], row[7]]
        else:
            print("Probably something wrong")
    return person_dict


def compare(person_dict, sleep=2):
    no_match_list = []
    for k, v in person_dict.items():
        if isinstance(v[1], float):
            name = v[0]
        else:
            if k[1] != "zh":
                name = v[0] + " " + v[1]
            else:
                name = v[0] + v[1]
        q_ids = _get_q_ids(name, k[1])
        print("-----------\n", v, q_ids)
        if q_ids is None:
            no_match_list.append((k, v))
            print("A match: ", k, v)
            continue
        person_wiki_dict = _sparql(q_ids, sleep)
        if not person_wiki_dict:
            no_match_list.append((k, v))
            print("A match: ", k, v)
            continue
        if 'gender' in person_wiki_dict:
            if person_wiki_dict['gender'] != v[2]:
                no_match_list.append((k, v))
                print("A match: ", k, v)
                continue
        if 'birthyear' in person_wiki_dict:
            if person_wiki_dict['birthyear'] != v[3]:
                no_match_list.append((k, v))
                print("A match: ", k, v)
                continue
        if 'deathyear' in person_wiki_dict:
            if person_wiki_dict['deathyear'] != v[4]:
                no_match_list.append((k, v))
                print("A match: ", k, v)
                continue
    return no_match_list


def _sparql(q_ids, sleep=2):
    if len(q_ids) == 0:
        return []
    person_wiki = {}

    with requests.Session() as s:
        for q in q_ids:
            response = s.get(URL, params={"format": "json", "query": QUERY1 + q + QUERY2})
            if response.status_code == 200:  # a successful response
                results = response.json().get("results", {}).get("bindings")
                if results:
                    for r in results:
                        if "date_of_birth" in r:
                            person_wiki["birthyear"] = r['date_of_birth']['value'][0:4]
                        if "date_of_death" in r:
                            person_wiki["deathyear"] = r['date_of_death']['value'][0:4]
                        if "gender" in r:
                            if r['gender']['value'] == "http://www.wikidata.org/entity/Q6581097":
                                gender = "male"
                            elif r['gender']['value'] == "http://www.wikidata.org/entity/Q6581072":
                                gender = "female"
                            person_wiki["gender"] = gender
            time.sleep(sleep)
    return person_wiki


def _get_q_ids(lookup, lang):
    """
    A function to search qnames in wikidata with a lookup string.
    :param lookup: a string
    :return: a list of string(s) (first 10 if more than 10)
    """
    q_ids = []
    with requests.Session() as s:
        response = s.get(URL, params={"format": "json", "query": QUERY_PERSON_NAME.format(lookup, lang)})
        if response.status_code == 200:  # a successful response
            results = response.json().get("results", {}).get("bindings")
            if results:
                for person in results:
                    q_ids.append(person['person']['value'][31:])
        return q_ids[0:10]

if __name__ == "__main__":
    person_dict = read_person_csv("https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Person.csv")
    # print(person_dict)

    no_match_list = compare(person_dict, 2)
    print(no_match_list)
    print("-------length of the no_match_list", len(no_match_list))


    # sample_dict = {('AG0619', 'en'): ['Qu', 'Yuan', 'male', '-0340', '-0278'], ('AG0619', 'zh'): ['屈', '原', 'male', '-0340', '-0278'], ('AG0620', 'en'): ['Qu', 'Qiubai', 'male', '1899', '1935'], ('AG0620', 'zh'): ['瞿', '秋白', 'male', '1899', '1935']}
    # no_match_list_for_sample = compare(sample_dict, 20)
    # print(no_match_list_for_sample)
    # print("-------length of the no_match_list", len(no_match_list_for_sample))





