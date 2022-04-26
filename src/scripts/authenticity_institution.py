"""
This is a python script to check authenticity of Named Entities in /Readact/csv/CSV and in SCB.
Main idea:
- Read ReadAct CSV files, get lookups
- Get the Q-identifier with library wikibaseintegrator for each lookup
- Use SPARQL to retrieve the property we need
- Compare wikidata item properties with CSV in ReadAct
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

# Plan to use only one SPARQL query for institutions.
# Not finished yet.
QUERY = """
SELECT DISTINCT  ?inst ?instLabel ?headquartersLabel ?administrativeTerritorialEntityLabel 
?locationOfFormationLabel ?inceptionLabel
WHERE
  {{
    ?item wdt:P31 wd:Q2085381 ;
          rdfs:label "新华书店"@zh .
    OPTIONAL
      { ?inst  wdt:P159  ?headquarters }
    OPTIONAL
      { ?inst  wdt:P131  ?administrativeTerritorialEntity }
    OPTIONAL
      { ?inst wdt:P740   ?locationOfFormation }
    OPTIONAL
      { ?inst wdt:P571  ?inception }
    SERVICE wikibase:label
      { bd:serviceParam wikibase:language  "[AUTO_LANGUAGE], en" }
  }}
GROUP BY ?inst ?instLabel ?headquartersLabel ?administrativeTerritorialEntityLabel 
?locationOfFormationLabel  ?inceptionLabel 
LIMIT 150
"""


def read_institution_csv(
    inst_url="https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Institution.csv",
):
    """
    A function to read "Person.csv".
    :param filename: "Person.csv".
    :return: a dictionary: key: unique person_id; value: [family_name,first_name,name_lang,sex,birthyear,deathyear]
    "name_lang" is used to decide if white space needs to be added into name or not.
    """
    df = pd.read_csv(inst_url, error_bad_lines=False)
    df = df.fillna("")
    inst_dict = {}
    place_dict = read_space_csv()
    for index, row in df.iterrows():
        if row[1] not in inst_dict:
            # key: string: inst_name
            # value: list: [place,start,end]
            if row[3] in place_dict:
                inst_dict[(row[0], row[1])] = [place_dict[row[3]][0], row[4], row[5]]
            else:
                inst_dict[(row[0], row[1])] = [row[3], row[4], row[5]]
                print("Please check. A space_id is not in Space.csv.")
        else:
            print("Please check. There are overlaps between institution names.")
    return inst_dict


def compare(inst_dict, sleep=2):
    no_match = {}
    match = {}
    for k, v in inst_dict.items():
        results = __get_QID(k[1])
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
                # print(results)
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
            # print("------------\nTime to sleep. 10 seconds~ ")
            time.sleep(sleep)
    # print("inst_wiki: ", inst_wiki)
    return inst_wiki


# def _get_q_ids(lookup=None):
#     """
#     A function to search qnames in wikidata with a lookup string.
#     :param lookup: a string
#     :return: a list of item identifiers (first 10)
#     """
#     e = wbi_core.FunctionsEngine()
#     instance = e.get_search_results(search_string=lookup, search_type="item")
#     if len(instance) > 0:
#         return instance[0:1]
#     else:
#         print("Lookup '", lookup, "' not in Wikidata. Didn't find Q-ids.")
#         return None


def __get_QID(lookup):
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
        return results[0:1]  # Return the first Qids


if __name__ == "__main__":
    inst_dict = read_institution_csv(
        "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data"
        "/Institution.csv"
    )

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

    """
    no_match_list:  [('Association of overseas Chinese in Vietnam', ['unknown', nan, nan]), ('越南华侨协会', ['unknown', 
    nan, nan]), ('English Language Services', ['unknown', nan, nan]), ('Central Committee of the Communist Party in 
    China', ['Beijing', 1921.0, nan]), ('Baiyangdian literary society', ['Baiyangdian', 1968.0, 1976.0]), ('白洋淀派', 
    ['Baiyangdian', 1968.0, 1976.0]), ('School of Foreign Studies', ['Nanjing', 1917.0, nan]), ('Beijing No. 4 High 
    School', ['Beijing', 1906.0, nan]), ('中国共产党', ['Beijing', 1921.0, nan]), ('Voice of America', ['Washington D.C.', 
    1942.0, nan]), ('Hubei Opera and Dance Theatre', ['Hubei', 1952.0, nan]), ('湖北省歌舞剧院', ['Hubei', 1952.0, nan]), 
    ('Jin Opera Group of Liulin County, Shanxi Province', ['Warszawa', nan, nan]), ('山西省柳林县晋剧团', ['Warszawa', nan, 
    nan]), ('Hunan Federation of Literary and Art Circles', ['Changsha', 1950.0, nan]), ('湖南省文学艺术界联合会', ['Changsha', 
    1950.0, nan]), ('Jiangxi Jiujiang Cultural Work Group', ['Jiujiang', nan, nan]), ('江西九江文工团', ['Jiujiang', nan, 
    nan]), ('Shanghai Youth Art Theatre', ['Shanghai', nan, nan]), ('上海青年艺术剧院', ['Shanghai', nan, nan]), 
    ('Beijing Peking Opera Troupe', ['Beijing', 1955.0, nan]), ('北京京剧团', ['Beijing', 1955.0, nan]), ('China Central 
    Ballet Company', ['Beijing', 1959.0, nan]), ('Hunan Opera and Dance Theatre', ['Changsha', 1953.0, nan]), 
    ('湖南省歌舞剧院', ['Changsha', 1953.0, nan]), ('Lu Xun Academy of Fine Arts', ["Yan'an", 1938.0, nan]), ('鲁迅艺术学院', 
    ["Yan'an", 1938.0, nan]), ('The Commercial Press', ['Shanghai', 1897.0, nan]), ('商务印书馆', ['Shanghai', 1897.0, 
    nan]), ('中国美术家协会', ['Beijing', 1949.0, nan]), ("People's Liberation Army of China", ['Beijing', 1927.0, nan]), 
    ('World Economic Research Institute', ['Shanghai', 1964.0, nan]), ('世界经济研究所', ['Shanghai', 1964.0, nan]), 
    ('Xinhua Bookstore', ["Yan'an", 1937.0, nan]), ('Xinhua Bookstore', ["Yan'an", 1937.0, nan]), ('新华书店', ["Yan'an", 
    1937.0, nan]), ('新华书店', ["Yan'an", 1937.0, nan])]
    
    length of the no_match_list: 37
    """

"""
no_match (with MediaWiki API, 2022-04-24):  
no_match dictionary:  {('AG0627', 'Association of overseas Chinese in Vietnam'): ['unknown', '', ''], ('AG0627', '越南华侨协会'): ['unknown', '', ''], ('AG0629', 'English Language Services'): ['unknown', '', ''], ('AG0630', 'Central Committee of the Communist Party in China'): ['Beijing', 1921.0, ''], ('AG0630', '中国共产党中央委员会'): ['Beijing', 1921.0, ''], ('AG0630', '中国共产党中央执行委员会'): ['Beijing', 1922.0, 1927.0], ('AG0631', 'Baiyangdian literary society'): ['Baiyangdian', 1968.0, 1976.0], ('AG0631', '白洋淀派'): ['Baiyangdian', 1968.0, 1976.0], ('AG0632', 'School of Foreign Studies'): ['Nanjing', 1917.0, ''], ('AG0633', 'Beijing No. 4 High School'): ['Beijing', 1906.0, ''], ('AG0633', '北京市第四中学'): ['Beijing', 1906.0, ''], ('AG0634', 'The Communist Party of China'): ['Beijing', 1921.0, ''], ('AG0634', '中国共产党'): ['Beijing', 1921.0, ''], ('AG0635', 'Voice of America'): ['Washington D.C.', 1942.0, ''], ('AG0638', 'Hubei Opera and Dance Theatre'): ['Hubei', 1952.0, ''], ('AG0638', '湖北省歌舞剧院'): ['Hubei', 1952.0, ''], ('AG0639', 'Jin Opera Group of Liulin County, Shanxi Province'): ['Warszawa', '', ''], ('AG0639', '山西省柳林县晋剧团'): ['Warszawa', '', ''], ('AG0640', 'China Writers Association'): ['Beijing', 1949.0, ''], ('AG0640', '中国作家协会'): ['Beijing', 1949.0, ''], ('AG0641', 'Hunan Federation of Literary and Art Circles'): ['Changsha', 1950.0, ''], ('AG0641', '湖南省文学艺术界联合会'): ['Changsha', 1950.0, ''], ('AG0642', 'Jiangxi Jiujiang Cultural Work Group'): ['Jiujiang', '', ''], ('AG0642', '江西九江文工团'): ['Jiujiang', '', ''], ('AG0643', 'Shanghai Youth Art Theatre'): ['Shanghai', '', ''], ('AG0643', '上海青年艺术剧院'): ['Shanghai', '', ''], ('AG0644', 'Beijing Peking Opera Troupe'): ['Beijing', 1955.0, ''], ('AG0644', '北京京剧团'): ['Beijing', 1955.0, ''], ('AG0645', 'China Central Ballet Company'): ['Beijing', 1959.0, ''], ('AG0645', '中央芭蕾舞团'): ['Beijing', 1959.0, ''], ('AG0646', 'Hunan Opera and Dance Theatre'): ['Changsha', 1953.0, ''], ('AG0646', '湖南省歌舞剧院'): ['Changsha', 1953.0, ''], ('AG0647', 'Lu Xun Academy of Fine Arts'): ["Yan'an", 1938.0, ''], ('AG0647', '鲁迅艺术学院'): ["Yan'an", 1938.0, ''], ('AG0648', 'Shanghai Ballet Company'): ['Shanghai', 1966.0, ''], ('AG0648', '上海芭蕾舞团'): ['Shanghai', 1966.0, ''], ('AG0650', 'The Commercial Press'): ['Shanghai', 1897.0, ''], ('AG0650', '商务印书馆'): ['Shanghai', 1897.0, ''], ('AG0653', "People's Liberation Army of China"): ['Beijing', 1927.0, ''], ('AG0654', 'World Economic Research Institute'): ['Shanghai', 1964.0, ''], ('AG0654', '世界经济研究所'): ['Shanghai', 1964.0, '']}
length of the no_match dictionary:  41
"""
