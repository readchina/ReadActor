"""
This is a python script to check authenticity of Named Entities of Institution type in ReadAct.
Strategy:
- Read Institution.csv in ReadAct, get lookups
- Get the QIDs by using MediaWiki API service
- Use SPARQL to retrieve properties from the found QIDs
- Compare wikidata item properties with data in the CSV table

The standards of verificating an Institution entry is:
to find one match among headquarters/administrativeTerritorialEntity/locationOfFormation/inception.
"""
import json
import time

import pandas as pd
import requests

from src.scripts.authenticity_space import read_space_csv

MEDIAWIKI_API_URL = "https://www.wikidata.org/w/api.php"
URL = "https://query.wikidata.org/sparql"
QUERY1 = """
PREFIX  schema: <http://schema.org/>
PREFIX  bd:   <http://www.bigdata.com/rdf#>
PREFIX  wdt:  <http://www.wikidata.org/prop/direct/>
PREFIX  wikibase: <http://wikiba.se/ontology#>

SELECT DISTINCT  ?item ?itemLabel ?headquartersLabel ?administrativeTerritorialEntityLabel 
?locationOfFormationLabel ?inceptionLabel
WHERE
  {{ ?article schema:about ?item ;
    FILTER ( ?item = <http://www.wikidata.org/entity/"""

QUERY2 = """> )
    OPTIONAL
      { ?item  wdt:P159  ?headquarters }
    OPTIONAL
      { ?item  wdt:P131  ?administrativeTerritorialEntity }
    OPTIONAL
      { ?item wdt:P740   ?locationOfFormation }
    OPTIONAL
      { ?item wdt:P571  ?inception }
    SERVICE wikibase:label
      { bd:serviceParam wikibase:language  "[AUTO_LANGUAGE], en"
      }
  }}
GROUP BY ?item ?itemLabel ?headquartersLabel ?administrativeTerritorialEntityLabel 
?locationOfFormationLabel  ?inceptionLabel 
"""


def read_institution_csv(
    inst_url="https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Institution.csv",
):
    """
    A function to read "Institution.csv"
    :param inst_url: the GitHub address of Institution.csv
    :return: a dictionary
    """
    df = pd.read_csv(inst_url, error_bad_lines=False)
    df = df.fillna("")
    ins_dict = {}
    place_dict = read_space_csv()
    for index, row in df.iterrows():
        if row[1] not in ins_dict:
            if row[3] in place_dict:
                ins_dict[(row[0], row[1])] = [place_dict[row[3]][0], row[4], row[5]]
            else:
                ins_dict[(row[0], row[1])] = [row[3], row[4], row[5]]
                print("Please check. A space_id is not in Space.csv.")
        else:
            print("Please check. There are overlaps between institution names.")
    return ins_dict


def compare(inst_dict, sleep=2):
    no_match = {}
    match = {}
    for k, v in inst_dict.items():
        results = get_QID(k[1])
        if results is None:
            no_match[k] = v
            continue
        q_ids = [x["id"] for x in results if x is not None]
        if q_ids is None:
            no_match[k] = v
            continue
        inst_wiki_dict = __sparql(q_ids, sleep)
        if (
            len(inst_wiki_dict["headquarters"]) > 0
            and v[0] in inst_wiki_dict["headquarters"]
        ):
            match[k] = v + q_ids
            print("A match: ", k, v + q_ids)
        elif (
            len(inst_wiki_dict["administrativeTerritorialEntity"]) > 0
            and v[0] in inst_wiki_dict["administrativeTerritorialEntity"]
        ):
            match[k] = v + q_ids
            print("A match: ", k, v + q_ids)
        elif (
            len(inst_wiki_dict["locationOfFormation"]) > 0
            and v[0] in inst_wiki_dict["locationOfFormation"]
        ):
            match[k] = v + q_ids
            print("A match: ", k, v + q_ids)
        elif (
            len(inst_wiki_dict["inception"]) > 0
            and str(v[1])[0:4] in str(inst_wiki_dict["inception"])[0:4]
        ):
            match[k] = v + q_ids
            print("A match: ", k, v + q_ids)
        else:
            no_match[k] = v
    return no_match, match


def __sparql(q_ids, sleep=2):
    if q_ids is None:
        return []
    inst_wiki = {}
    with requests.Session() as s:
        for q in q_ids:
            response = s.get(
                URL, params={"format": "json", "query": QUERY1 + q + QUERY2}
            )
            if response.status_code == 200:  # a successful response
                results = response.json().get("results", {}).get("bindings")
                (
                    inst_wiki["name"],
                    inst_wiki["headquarters"],
                    inst_wiki["administrativeTerritorialEntity"],
                    inst_wiki["locationOfFormation"],
                    inst_wiki["inception"],
                    inst_wiki["QID"],
                ) = ([], [], [], [], [], [])
                if q is not None:
                    inst_wiki["QID"] = q
                if results:
                    for b in results:
                        if "itemLabel" in b:
                            inst_wiki["name"] = b["itemLabel"]["value"]
                        if "headquartersLabel" in b:
                            headquarters = b["headquartersLabel"]["value"]
                            inst_wiki["headquarters"].append(headquarters)
                        if "administrativeTerritorialEntityLabel" in b:
                            administrativeTerritorialEntity = b[
                                "administrativeTerritorialEntityLabel"
                            ]["value"]
                            inst_wiki["administrativeTerritorialEntity"].append(
                                administrativeTerritorialEntity
                            )
                        if "locationOfFormationLabel" in b:
                            locationOfFormation = b["locationOfFormationLabel"]["value"]
                            inst_wiki["locationOfFormation"].append(locationOfFormation)
                        if "inceptionLabel" in b:
                            inception = b["inceptionLabel"]["value"]
                            inst_wiki["inception"].append(inception)
            time.sleep(sleep)
    return inst_wiki


def get_QID(lookup):
    params = {
        "action": "wbsearchentities",
        "language": "en",
        "search": lookup,
        "format": "json",
        "limit": 10,
    }
    reply = requests.get(MEDIAWIKI_API_URL, params=params)
    reply.raise_for_status()
    search_results = reply.json()

    results = []
    if search_results["success"] != 1:
        return None
    else:
        for i in search_results["search"]:
            results.append({"id": i["id"], "label": i["label"]})
    if len(results) == 0:
        return None
    else:
        return results[
            0:1
        ]  # Return the first Qid. In the future, to increase the accuracy of match, just rewrite
        # this line to allow more QIDs to be returned for comparing.


if __name__ == "__main__":

    inst_dict = read_institution_csv()
    print(inst_dict)
    no_match, match = compare(inst_dict, 10)
    print("no_match dictionary: ", no_match)
    print("length of the no_match dictionary: ", len(no_match))

    print("=======\n\n\n\n\n")
    print("match dictionary: ", match)
    print("length of the match dictionary: ", len(match))

    d = {}
    for k, v in match.items():
        d[k[0]] = [k[1]] + v
    with open("../results/match_for_institution.json", "w") as f:
        json.dump(d, f)
