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
from authenticity_space import read_space_csv

def read_institution_csv(inst_url="https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data"
                                       "/Institution.csv"):
    """
    A function to read "Person.csv".
    :param filename: "Person.csv".
    :return: a dictionary: key: unique person_id; value: [family_name,first_name,name_lang,sex,birthyear,deathyear]
    "name_lang" is used to decide if white space needs to be added into name or not.
    """
    df = pd.read_csv(inst_url, error_bad_lines=False)
    inst_dict = {}
    place_dict = read_space_csv()
    for index, row in df.iterrows():
        if row[1] not in inst_dict:
            # key: string: inst_name
            # value: list: [place,start,end]
            if row[3] in place_dict:
                inst_dict[row[1]] = [place_dict[row[3]][0], row[4], row[5]]
            else:
                inst_dict[row[1]] = [row[3], row[4], row[5]]
                print("Please check. A space_id is not in Space.csv.")
        else:
            print("Please check. There are overlaps between institution names.")
    return inst_dict


def compare(person_dict, sleep=2):
    no_match_list = []
    for k, v in inst_dict.items():
        if v[1] == "unknown":
            continue
        q_ids = _get_q_ids(k)
        # print("-------")
        # print(q_ids)
        # print("k: ", k)
        if q_ids is None:
            no_match_list.append((k, v))
            continue
        inst_wiki_dict = _sparql(q_ids, sleep)
        # print("inst_wiki_dict: ", inst_wiki_dict)
        if not inst_wiki_dict:
            no_match_list.append((k, v))
            continue
        if 'headquarters' in inst_wiki_dict:
            for h in inst_wiki_dict['headquarters']:
                if h != v[0]:
                    no_match_list.append((k, v))
                    continue
        if 'administrativeTerritorialEntity' in inst_wiki_dict:
            for h in inst_wiki_dict['administrativeTerritorialEntity']:
                if h != v[0]:
                    no_match_list.append((k, v))
                    continue
        if 'locationOfFormation' in inst_wiki_dict:
            for h in inst_wiki_dict['locationOfFormation']:
                if h != v[0]:
                    no_match_list.append((k, v))
                    continue
        if 'inception' in inst_wiki_dict:
            for h in inst_wiki_dict['inception']:
                if h != v[1]:
                    no_match_list.append((k, v))
                    continue
        time.sleep(sleep)
    return no_match_list


def _sparql(q_ids, sleep=2):
    if q_ids is None:
        return []
    person_wiki_dict = {}
    for index, q in enumerate(q_ids):
        query = """
        PREFIX  schema: <http://schema.org/>
        PREFIX  bd:   <http://www.bigdata.com/rdf#>
        PREFIX  wdt:  <http://www.wikidata.org/prop/direct/>
        PREFIX  wikibase: <http://wikiba.se/ontology#>
        
        SELECT DISTINCT  ?item ?itemLabel ?headquartersLabel ?administrativeTerritorialEntityLabel 
        ?locationOfFormationLabel ?inceptionLabel
        WHERE
          { ?article  schema:about       ?item ;
            FILTER ( ?item = <http://www.wikidata.org/entity/Q849418> )
            OPTIONAL
              { ?item  wdt:P159  ?headquarters }
            OPTIONAL
              { ?item  wdt:P131  ?administrativeTerritorialEntity }
            OPTIONAL
              { ?item wdt:P740   ?locationOfFormation }
            OPTIONAL
              { ?item wdt:P571  ?inception }
            SERVICE wikibase:label
              { bd:serviceParam wikibase:language  "en, ch"
              }
          }
        GROUP BY ?item ?itemLabel ?headquartersLabel ?administrativeTerritorialEntityLabel ?locationOfFormationLabel  ?inceptionLabel
        """
        sparql = SPARQLWrapper("https://query.wikidata.org/sparql")

        sparql.setQuery(query)
        sparql.setReturnFormat(JSON)
        results = sparql.query().convert()
        # print(results)

        person_wiki_dict["headquarters"],person_wiki_dict["administrativeTerritorialEntity"], person_wiki_dict[
            "locationOfFormation"],person_wiki_dict["inception"]  = [], [], [], []
        if results['results']['bindings']:
            bindings = results['results']['bindings']
            for b in bindings:
                if "headquartersLabel" in b:
                    headquarters = b['headquartersLabel']['value']
                    person_wiki_dict["headquarters"].append(headquarters)
                if "administrativeTerritorialEntityLabel" in b:
                    administrativeTerritorialEntity = b['administrativeTerritorialEntityLabel']['value']
                    person_wiki_dict["administrativeTerritorialEntity"].append(administrativeTerritorialEntity)
                if "locationOfFormationLabel" in b:
                    locationOfFormation = b['locationOfFormationLabel']['value']
                    person_wiki_dict["locationOfFormation"].append(locationOfFormation)
                if "inceptionLabel" in b:
                    inception = b['inceptionLabel']['value']
                    person_wiki_dict["inception"].append(inception)
        # print("person_wiki_dict: ", person_wiki_dict)
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
        print("Lookup '", lookup, "' not in Wikidata. Didn't find Q-ids.")
        return None


if __name__ == "__main__":
    inst_dict = read_institution_csv("https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data"
                                       "/Institution.csv")
    # print(inst_dict)
    no_match_list = compare(inst_dict, 5)
    print("no_match_list: ", no_match_list)
    print("-------length of the no_match_list", len(no_match_list))






