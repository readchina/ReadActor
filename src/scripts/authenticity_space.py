"""
This is a python script to check authenticity of Named Entities in /Readact/csv/CSV and in SCB.
- Get lookups from ReadAct
- Use openstreetmap API to filter those matched ones
- Use SPARQL to check the rest:
    - Get Q-identifiers for a lookup
    - retrieve the properties of coordinates from wikidata
    - Compare the retrieved coordinate with the coordinate stored in ReadAct
"""
import sys
import time

import pandas as pd
import requests

URL = "https://query.wikidata.org/sparql"

MEDIAWIKI_API_URL = "https://www.wikidata.org/w/api.php"

QUERY_COORDINATE = """
SELECT DISTINCT ?item ?coordinate
WHERE {{
  values ?item {{ wd:{} }}
  OPTIONAL {{ ?item  wdt:P625  ?coordinate . }}

  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en". }}
}}
"""


def read_space_csv(
    space_url="https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Space.csv",
):
    """
    A function to read "Space.csv" for now.
    :param filename: "Space.csv" for now
    :return: a dictionary of coordinate locations
    """
    df = pd.read_csv(space_url, error_bad_lines=False)
    geo_code_dict = {}
    for index, row in df.iterrows():
        # consider the case that if there are identical space_names in csv file
        if row[0] not in geo_code_dict:
            # key: space_id
            # value: space_name, space_type, lat, lang
            geo_code_dict[row[0]] = [row[2], row[3], row[5], row[6]]
        else:
            print("Space id not exist ?!")
    return geo_code_dict


def compare_to_openstreetmap(geo_code_dict):
    """
    A function to use the lat/lon from CSV file as lookup, to see if space_name is part of the returned message
    :param dictionary_value: tuple: space_name, lat, lon
    :return: None if match, a string message if not match
    """
    no_match_list = []
    for k, v in geo_code_dict.items():
        if v[0] != "unknown" and v[2] != 0.0:
            lat = str(v[2])
            lon = str(v[3])
            url = (
                "https://nominatim.openstreetmap.org/reverse?format=xml&lat="
                + lat
                + "&lon="
                + lon
                + "&zoom=18&addressdetails=1&format=json&accept-language=en"
            )
            data = requests.get(url)
            if v[0].lower() not in str(data.json()).lower():
                no_match_list.append(v)
    return no_match_list


def geo_code_compare(no_match_list):
    """
    For geo locations in Space.csv, compare latitude/longitude for matching via retrieve CSV from wikidata.
    :param geo_code_dict: key: unique (lat,long) tuples; value: space_name in csv
    :return: None or list of entries which can't match
    """
    still_no_match_list = []
    count = 0
    for i in no_match_list:
        print("-------\n", i)
        if i[0] is None:
            res = None
        else:
            count += 1
            res = __get_QID(i[0])
            print("res: ", res)

        if count == 20:
            time.sleep(30)
            count = 0

        if res is None:
            still_no_match_list.append(i)
        else:
            coordinate_list = __get_coordinate_from_wikidata(res['id'])
            print("coordinate_list: ", coordinate_list)
            # if no coordinate_list, collect item into list, break nested loop
            if len(coordinate_list) == 0:
                still_no_match_list.append(i)
                continue
            for c in coordinate_list:
                # If the difference are within +-0.9, consider a match, no collection, break nested loop
                # Pay attention that Wikidata coordinate have the longitude first, and the latitude later. It is the
                # opposite in ReadAct if we read the table from left to right.
                if (
                    float(abs(float(c[0]))) - 0.9
                    <= float(i[3])
                    <= float(abs(float(c[0]))) + 0.9
                ) and (
                    float(abs(float(c[1]))) - 0.9
                    <= float(i[2])
                    <= float(abs(float(c[1]))) + 0.9
                ):
                    i = ""
                    break
            if len(i) > 0:
                still_no_match_list.append(i)

    if len(still_no_match_list) != 0:
        print("still_no_match_list: ", still_no_match_list)
        return still_no_match_list


def __get_QID(
    lookup
):
    params = {
        "action": "wbsearchentities",
        "language": 'en',
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
            results.append({'id': i['id'], 'label': i['label']})
    if len(results) == 0:
        return None
    else:
        return results[0]


def __get_coordinate_from_wikidata(q):
    """
    A function to extract coordinate location(if exists) of a wikidata entity
    :param qname: a list of Qname
    :return: a list with tuples, each tuple is a (lat, long) combination
    """
    coordinate_list =[]
    with requests.Session() as s:
        response = s.get(
            URL,
            params={
                "format": "json",
                "query": QUERY_COORDINATE.format(q),
            },
        )
        if response.status_code != 200:
            print(response.status_code)
            print("===============")
        if response.status_code == 200:  # a successful response
            results = response.json().get("results", {}).get("bindings")

            print(results)
            if len(results) == 0:
                pass
            else:
                for r in results:
                    # If this entity is not recorded in this space_wiki dictionary yet:
                    if "coordinate" in r:
                        if "value" in r["coordinate"]:
                            c = r["coordinate"]["value"][6:-1].split()
                            # for example, '[114.158611111,22.278333333]'
                            coordinate_list.append(c)
    return coordinate_list


if __name__ == "__main__":

    # To compare the extracting coordinate location with the info in Space.csv
    space_url = (
        "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Space.csv"
    )
    geo_code_dict = read_space_csv(space_url)

    # To filter CSV entries with comparing to openstreetmap first
    no_match_list = compare_to_openstreetmap(geo_code_dict)

    print(no_match_list)
    # no_match_list = [['Ningbo', 'PL', 29.868336, 121.54399], ['Dréan', 'PL', 36.6848, 7.7511], ['Hankou', 'PL',
    # 30.541831166, 114.32583203], ['Three Gorges Dam', 'L', 30.81329, 111.014292],  ['Rugao', 'PL', 22.74024,
    # 120.49042], ['Warszawa', 'PL', 52.229675, 21.01223]]

    # To compare the rest with wikidata info
    still_no_match_list = geo_code_compare(no_match_list)
