"""
This is a python script to cross-reference Person entities in ReadAct with wikidata.
Strategy:
- Read ReadAct CSV files, get names (or Wikidata link) as lookups
- Get the QIDs: look up with name (or look up with Wikipedia link (if available))
- Use SPARQL to retrieve properties from the found QIDs
- Compare wikidata item properties with data in the CSV table

"""
import json
import time
from itertools import islice

import pandas as pd
import requests

from src.scripts.authenticity_space import read_space_csv

URL = "https://query.wikidata.org/sparql"
QUERY = """
        SELECT ?person ?personLabel ?ybirth ?ydeath ?birthplaceLabel ?genderLabel
        WHERE {{ 
        {{?person wdt:P31 wd:Q5 ;
                rdfs:label "{}"@{} . }} UNION {{?person wdt:P31 wd:Q5 ;
                skos:altLabel "{}"@{} . }}
        OPTIONAL {{ ?person  wdt:P21  ?gender . }}
        OPTIONAL {{ ?person  wdt:P569  ?birth . BIND(year(?birth) as ?ybirth) }}
        OPTIONAL {{ ?person  wdt:P570  ?death . BIND(year(?death) as ?ydeath) }}
        OPTIONAL {{ ?person wdt:P19  ?birthplace . }}
        OPTIONAL {{ ?person skos:altLabel ?altLabel . }}
        
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language  "[AUTO_LANGUAGE], en"}}
        }}
        GROUP BY ?person ?personLabel ?ybirth ?ydeath ?birthplaceLabel ?genderLabel
        LIMIT 250
        """

QUERY_WITH_QID = """
SELECT ?person ?personLabel ?ybirth ?ydeath ?birthplaceLabel ?genderLabel
WHERE {{ 
  values ?person {{wd:{} }}
        OPTIONAL {{ ?person  wdt:P21  ?gender . }}
        OPTIONAL {{ ?person  wdt:P569  ?birth . BIND(year(?birth) as ?ybirth) }}
        OPTIONAL {{ ?person  wdt:P570  ?death . BIND(year(?death) as ?ydeath) }}
        OPTIONAL {{ ?person wdt:P19  ?birthplace . }}
        OPTIONAL {{ ?person skos:altLabel ?altLabel . }}
        
        SERVICE wikibase:label {{ bd:serviceParam wikibase:language  "[AUTO_LANGUAGE], en"}}
        }}
LIMIT 1
"""


#################################################################
################## Approach 1 : look up with name ##################
def read_person_csv(
    person_url="https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Person.csv",
):
    """
    A function to read "Person.csv", preprocess CSV.
    :param person_url
    :return: a dictionary
    """
    df = pd.read_csv(person_url, error_bad_lines=False).fillna("")
    person_dict = {}
    place_dict = read_space_csv()
    for index, row in df.iterrows():
        id = row["person_id"]
        print("-----\n", index, id)
        # a dictionary to collect final q_id for each unique person id
        if id not in person_dict:
            person_dict[id] = dict()

        if row["language"] not in person_dict[row["person_id"]]:
            # name_ordered is a list of a single name or multiple names
            name_ordered = order_name_by_language(row)

        # sex or gender type in Wikidata for human: male, female, non-binary, intersex, transgender female,
        # transgender male, agender.
        if row["sex"] not in [
            "male",
            "female",
            "non-binary",
            "intersex",
            "transgender female",
            "transgender ",
            "male",
            "agender",
        ]:
            row["sex"] = ""
        else:
            row["sex"] = row["sex"].strip()

        # birth_years and death_years are two lists of a single year or multiple years
        birth_years = __clean_birth_death_year_format(row["birthyear"])
        death_years = __clean_birth_death_year_format(row["deathyear"])

        if type(row["alt_name"]) != str:
            row["alt_name"] = ""
        else:
            row["alt_name"] = row["alt_name"].strip()

        # Replace space_id with the name of space
        if row["place_of_birth"] in place_dict:
            row["place_of_birth"] = place_dict[row["place_of_birth"]][0]
        else:
            print(
                "Please check why the place of birth is not in the dictionary of space."
            )

        person_dict[id][row["language"]] = [
            name_ordered,
            row["sex"],
            birth_years,
            death_years,
            row["alt_name"],
            row["place_of_birth"],
        ]
    return person_dict


def order_name_by_language(row):
    if type(row["family_name"]) != str:
        name_ordered = [row["first_name"]]
    elif type(row["first_name"]) != str:
        name_ordered = [row["family_name"]]
    elif row["language"] == "zh":
        name_ordered = [row["family_name"] + row["first_name"]]  # 毛泽东
    else:
        # Make it a list with two types of order to suit different languages
        # Since the non-Chinese names are the minority, the influence on processing speed can be tolerant
        # For example:
        # pinyin name: family name + " " + first name
        # Korean name: family name + " " + first name
        # Russian name: first name + " " + family name
        name_ordered = [
            row["first_name"] + " " + row["family_name"],
            row["family_name"] + " " + row["first_name"],
        ]
    return name_ordered


def __clean_birth_death_year_format(default_year):
    return default_year
    # char_to_remove = ["[", "]", "?", "~"]
    # cleaned_year = default_year
    # for c in char_to_remove:
    #     cleaned_year = cleaned_year.replace(c, "")
    #
    # if cleaned_year.isalpha():  # "XXXX" contain 0 information
    #     years = []
    # elif "." in cleaned_year:
    #     cleaned_year = cleaned_year.split("..")
    #     years = list(range(int(cleaned_year[0]), int(cleaned_year[1]) + 1))
    # elif "-" in cleaned_year:  # For BCE year
    #     cleaned_year = int(cleaned_year.replace("-", ""))
    #     years = [cleaned_year + 1, cleaned_year, cleaned_year - 1]
    # elif any([i.isalpha() for i in cleaned_year]):
    #     cleaned_year = [
    #         cleaned_year.replace("X", "0").replace("x", "0"),
    #         cleaned_year.replace("X", "9").replace("x", "0"),
    #     ]
    #     # Note(QG): Maybe consider to tickle the weight at this step already? since range(1000,2000)
    #     # covers 1000 years and it does not offer really useful information
    #     years = list(range(int(cleaned_year[0]), int(cleaned_year[1]) + 1))
    # elif "," in cleaned_year:
    #     cleaned_year = cleaned_year.split(",")
    #     years = list(range(int(cleaned_year[0]), int(cleaned_year[1]) + 1))
    # else:
    #     years = [int(cleaned_year)]
    # return years


def get_person_weight(person_dict, sleep=2):
    person_weight_dict = {}
    for person_id, value in person_dict.items():
        languages = value.keys()
        l = []
        person_weight_dict[person_id] = []
        for lang in languages:
            v = value[lang]
            # Use the ordered_name list as lookups
            lookup_names = v[0]

            # If there are alt_name, add it into the list of names
            if len(value[lang][4]) != 0:
                lookup_names.append(v[4])

            if lookup_names != ["anonymous"] and lookup_names != ["无名"]:
                person = sparql_by_name(lookup_names, lang, sleep)
            else:
                continue

            if len(person) == 0:
                # There is no match
                pass

            else:
                weight = 0
                weights = []
                Qids = []
                wiki = []
                for Q_id, p in person.items():
                    # all the matched fields will add weight 1 to the total weight for this Q_id
                    if "gender" in p:
                        if p["gender"] == v[1]:
                            weight += 1
                    elif "birthyear" in p:
                        if p["birthyear"] in v[2]:
                            weight += 1
                    elif "deathyear" in p:
                        if p["deathyear"] in v[3]:
                            weight += 1
                    elif "birthplace" in p:
                        if p["birthplace"] == v[5]:
                            weight += 1

                    weights.append(weight)
                    Qids.append(Q_id)
                    wiki.append(p)
                    weight = 0

                l.append(lang)
                l.append(weights)
                l.append(Qids)
                l.append(wiki)

            if len(l) > 0:
                person_weight_dict[person_id].append(l)
            l = []
    return person_weight_dict


def sparql_by_name(lookup_names, lang, sleep=2):
    if len(lookup_names) == 0:
        return None
    person = (
        {}
    )  # To collect entities which is found for the same person with different names
    with requests.Session() as s:
        for lookup in lookup_names:
            # print("++++ For this name: \n", lookup)
            response = s.get(
                URL,
                params={
                    "format": "json",
                    "query": QUERY.format(lookup.strip(), lang, lookup, lang),
                },
            )
            if response.status_code == 200:  # a successful response
                results = response.json().get("results", {}).get("bindings")
                if len(results) == 0:
                    # Didn't find the entity with this name on Wikidata
                    continue
                else:
                    for r in results:
                        person_wiki = {}
                        # If this entity is not recorded in the person dictionary yet:
                        if r["person"]["value"][31:] not in person:
                            if "person" in r:
                                person_wiki["Q-id"] = r["person"]["value"][
                                    31:
                                ]  # for example, 'Q558744'
                            if "personLabel" in r:
                                person_wiki["name"] = r["personLabel"]["value"]
                            if "genderLabel" in r:
                                person_wiki["gender"] = r["genderLabel"]["value"]
                            if "ybirth" in r:
                                person_wiki["birthyear"] = r["ybirth"]["value"]
                            if "ydeath" in r:
                                person_wiki["deathyear"] = r["ydeath"]["value"]
                            if "birthplaceLabel" in r:
                                person_wiki["birthplace"] = r["birthplaceLabel"][
                                    "value"
                                ]
                            person[person_wiki["Q-id"]] = person_wiki
            time.sleep(sleep)
    return person


def compare_weights(person_weight_dict):
    no_match_person = []
    match_person = {}
    for id, p in person_weight_dict.items():
        # No match at all
        if len(p) < 1:
            no_match_person.append(id)
        # At least one match for one language, no match for the rest language
        elif len(p) == 1:
            weights = p[0][1]
            max_weight = max(
                weights
            )  # for multiple identical weights to be the max weight, return the first max weight
            index = weights.index(max_weight)
            Qid = p[0][2][index]
            wi = p[0][3][index]
            match_person[id] = [Qid, wi]
        # There are at least one match for each language
        else:
            m = 0
            q = ""
            w = ""
            for item in p:
                if len(item[1]) == 1:
                    max_weight = item[1][0]
                    Qid = item[2][0]
                    wiki = item[3][0]
                else:
                    weights = item[1]
                    max_weight = max(
                        weights
                    )  # for multiple identical weights to be the max weight, return the first
                    index = weights.index(max_weight)
                    Qid = item[2][index]
                    wiki = item[3][index]
                if max_weight > m:
                    m = max_weight
                    q = Qid
                    w = wiki
                match_person[id] = [q, w]
    return no_match_person, match_person


def chunks(person_dict, SIZE=30):
    it = iter(person_dict)
    for i in range(0, len(person_dict), 2):
        yield {k: person_dict[k] for k in islice(it, SIZE)}


########################################################################
################## Approach 2 : look up with Wikipedia link ##################
def get_matched_by_wikipedia_url(
    person_url="https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Person.csv",
):
    df = pd.read_csv(person_url, error_bad_lines=False)
    person_matched_by_wikipedia = {}

    count = 0
    for index, row in df.iterrows():
        id = row["person_id"]
        print(index, id)
        if id not in person_matched_by_wikipedia:
            Qid = get_Qid_from_wikipedia_url(row)
            # print("Qid: ", Qid)
            if Qid is not None:
                count += 1
                if count == 10:
                    time.sleep(90)
                    count = 0
                wiki = sparql_with_Qid(Qid)
                if len(wiki) > 0:
                    person_matched_by_wikipedia[id] = [Qid, wiki]
    return person_matched_by_wikipedia


def get_Qid_from_wikipedia_url(row):
    # link: the wikipedia link in Person.csv
    if isinstance(row["source_1"], str) and ".wikipedia.org/wiki/" in row["source_1"]:
        link = row["source_1"]
    elif isinstance(row["source_2"], str) and ".wikipedia.org/wiki/" in row["source_2"]:
        link = row["source_2"]
    else:
        return None
    if len(link) > 30:
        language = link[8:10]
        name = link[30:]
        # Use MediaWiki API to query
        url = (
            "https://"
            + language
            + ".wikipedia.org/w/api.php?action=query&prop=pageprops&titles="
            + name
            + "&format=json"
        )
        response = requests.get(url).json()
        if "pageprops" in list(response["query"]["pages"].values())[0]:
            pageprops = list(response["query"]["pages"].values())[0]["pageprops"]
            if "wikibase_item" in pageprops:
                Qid = list(response["query"]["pages"].values())[0]["pageprops"][
                    "wikibase_item"
                ]
                return Qid


def sparql_with_Qid(Qid):
    wiki_dict = {}
    with requests.Session() as s:
        response = s.get(
            URL, params={"format": "json", "query": QUERY_WITH_QID.format(Qid)}
        )
        if response.status_code == 200:  # a successful response
            results = response.json().get("results", {}).get("bindings")
            if len(results) == 0:
                print(
                    "Didn't find the entity with this Q-identifier \"",
                    Qid,
                    '" on Wikidata',
                )
                return None
            else:
                for r in results:
                    if r is not None:
                        wiki_dict["Q-id"] = Qid
                        if "personLabel" in r:
                            wiki_dict["name"] = r["personLabel"]["value"]
                        if "genderLabel" in r:
                            wiki_dict["gender"] = r["genderLabel"]["value"]
                        if "ybirth" in r:
                            wiki_dict["birthyear"] = r["ybirth"]["value"]
                        if "ydeath" in r:
                            wiki_dict["deathyear"] = r["ydeath"]["value"]
                        if "birthplaceLabel" in r:
                            wiki_dict["birthplace"] = r["birthplaceLabel"]["value"]
    return wiki_dict


if __name__ == "__main__":

    person_url = (
        "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Person.csv"
    )

    #################################################################
    ################## Approach 1 : look up with name ##################
    person_dict = read_person_csv(
        "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Person.csv"
    )

    # Break the entire dictionary into several chunks.
    # So that we can add break sessions in between to avoid exceed the limitation of SPARQL query
    no_match_by_name = []
    matched_by_name = {}
    for chunk in chunks(person_dict, 30):  # the digit here controls the batch size
        if len(chunk) > 0:
            print("chunk: \n", chunk)
            person_weight_dict = get_person_weight(chunk, 2)
            no_match, person_match_dict = compare_weights(person_weight_dict)
            if len(no_match) > 0:
                no_match_by_name = [*no_match_by_name, *no_match]
            if len(person_match_dict) > 0:
                matched_by_name = {**matched_by_name, **person_match_dict}

            print("\n----------\n")
            print("no match: ", no_match)
            print("person_match_dict: ", person_match_dict)

            print("\n===========================\n")
            print("Current final no match: ", no_match_by_name)
            print("Current final person_match_dict: ", matched_by_name)
            print("\n I am taking a break XD \n")

            time.sleep(
                90
            )  # for every a few person entries, let this script take a break of 90 seconds

    print("I finished all the iteration.")
    print("\n===========================\n")
    print("no match: ", no_match_by_name)
    print("person_match_dict: ", matched_by_name)

    with open("../results/no_match_by_querying_name.json", "w") as f:
        json.dump(no_match_by_name, f)

    with open("../results/matched_by_name.json", "w") as f:
        json.dump(matched_by_name, f)

    ########################################################################
    ################## Approach 2 : look up with wikidata link ##################
    # matched_by_wikipedia = get_matched_by_wikipedia_url("https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Person.csv")
    #
    # print('===========\n', matched_by_wikipedia)
    # with open('../results/matched_by_wikipedia.json', 'w') as f:
    #     json.dump(matched_by_wikipedia, f)

    #################################################################
    ################## Comparison ##################
    with open("../results/matched_by_name.json", "r") as f:
        name = json.load(f)
    with open("../results/matched_by_wikipedia.json", "r") as f:
        wikipedia = json.load(f)

    # Rearranged a dictionay to contain person information for later comparison
    person = read_person_csv(person_url)
    new_dict = {}
    for key in person.keys():
        names, gender, birthyear, deathyear, birthplace = [], [], [], [], []
        new_dict[key] = {}
        langs = [*person[key]]
        for lang in langs:
            names.extend(person[key][lang][0])
            if len(person[key][lang][4]) > 0:
                names.append(person[key][lang][4])
            gender.append(person[key][lang][1])
            birthyear.extend(person[key][lang][2])
            deathyear.extend(person[key][lang][3])
            birthplace.append(person[key][lang][5])
            new_dict[key]["name"] = list(set(names))
            new_dict[key]["gender"] = list(set(gender))
            new_dict[key]["birthyear"] = list(set(birthyear))
            new_dict[key]["deathyear"] = list(set(deathyear))
            new_dict[key]["birthplace"] = list(set(birthplace))

    longest_match = {}
    for key in new_dict.keys():
        score_name = 0
        score_wikipedia = 0
        if key in name and key in wikipedia:
            if name[key][1]["Q-id"] == wikipedia[key][1]["Q-id"]:
                longest_match[key] = name[key]
            else:
                for k in name[key][1].keys():
                    if k == "Q-id":
                        continue
                    if new_dict[key][k]:
                        if k in ["birthyear", "deathyear"]:
                            if int(name[key][1][k]) in new_dict[key][k]:
                                score_name += 1
                        else:
                            if name[key][1][k] in new_dict[key][k]:
                                score_name += 1

                for k in wikipedia[key][1].keys():
                    if k == "Q-id":
                        continue
                    if new_dict[key][k]:
                        if k in ["birthyear", "deathyear"]:
                            if int(wikipedia[key][1][k]) in new_dict[key][k]:
                                score_wikipedia += 1
                        else:
                            if wikipedia[key][1][k] in new_dict[key][k]:
                                score_wikipedia += 1

                if score_name > score_wikipedia:
                    longest_match[key] = name[key]
                elif score_name < score_wikipedia:
                    longest_match[key] = wikipedia[key]
                else:  # same score
                    print("Check manually.")
        elif key in name:
            longest_match[key] = name[key]
        elif key in wikipedia:
            longest_match[key] = wikipedia[key]

    with open("../results/match_after_choosing_longest_match.json", "w") as f:
        json.dump(longest_match, f)

    #################################################################
    ################## Difference between the results of two approaches ##################

    difference_between_two_approaches = []
    l = [x for x in wikipedia.keys() if x in name.keys()]

    print("number of intersection: ", len(l))  # 374
    for key in l:
        if name[key] != wikipedia[key]:
            print("\nFor person with id ", key, ":")
            print("name[key]: ", "\n", name[key])
            print("wikipedia[key]: ", "\n", wikipedia[key], "\n")
            difference_between_two_approaches.append(key)
