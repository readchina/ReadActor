"""
This is a python script to check authenticity of Named Entities of Space type in ReadAct.
Strategy:
- Read Space.csv in ReadAct, get space names and coordinates
- Search with coordinates in OpenStreetMap.
- If not match, use MediaWiki API service to look up with space name for QIDs
- Use SPARQL to retrieve properties from the found QIDs
- Compare wikidata item properties with data in the CSV table

The standards for macthing are:
1. With OpenStreetMap data, the lookup string should be contained in the location of the given coordinate.
2. With Wikidata, the difference between the coordinate in the CSV table and the coordinate in Wikidata should be
less or equal to 0.9.
"""

import json
import time
from itertools import islice

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
    space_url="https://raw.githubusercontent.com/readchina/ReadAct/2.0-RC-patch/csv/data/Space.csv",
):
    """
    A function to read "Space.csv" for now.
    :param space_url
    :return: a dictionary
    """
    df = pd.read_csv(space_url, error_bad_lines=False)
    geo_code_dict = {}
    for index, row in df.iterrows():
        # consider the case that if there are identical space_names in csv file
        if row[0] not in geo_code_dict:
            # key: space_id
            # value: space_name, space_type, lat, lang
            geo_code_dict[row[0]] = [row[3], row[2], row[5], row[6]]
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
        item = query_with_OSM(k, v)
        if item:  # item: space_name, space_type, lat, lang, space_id
            no_match_list.append(item)
    return no_match_list


def query_with_OSM(k, v):
    if v[0] != "unknown" and v[2] != 0.0:
        lat = str(v[2])
        long = str(v[3])
        url = (
            "https://nominatim.openstreetmap.org/reverse?format=xml&lat="
            + lat
            + "&lon="
            + long
            + "&zoom=18&addressdetails=1&format=json&accept-language=en"
        )
        data = requests.get(url)
        if v[0].lower() not in str(data.json()).lower():
            item = v + [k]
            return item


def geo_code_compare(no_match_list):
    """
    For geo locations in Space.csv, compare latitude/longitude
    :param geo_code_dict
    :return: None or list of entries which don't match
    """
    still_no_match_list = []
    space_with_QID = {}  # To collect QIDs for Space.csv
    count = 0
    for i in no_match_list:
        if i[0] is None:
            res = None
        else:
            count += 1
            res = get_QID(
                i[0]
            )  # If there are more than one returned QID and we want to check all of them,
            # the following code must be modified as well.

        if count == 20:
            time.sleep(30)
            count = 0

        if res is None:
            still_no_match_list.append(i)
        else:
            coordinate_list = get_coordinate_from_wikidata(res["id"])
            # if no coordinate_list, collect item into list, break nested loop
            if len(coordinate_list) == 0:
                still_no_match_list.append(i)
                continue
            for coordinate_wiki in coordinate_list:
                # If the difference are within +-0.9, consider a match, no collection for no_match_list,
                # but one collect action for space_with_QID dictionary, then break nested loop
                # Pay attention that the query-returned coordinate have order: long, lat.
                if compare_coordinates_with_threhold(coordinate_wiki, i[2], i[3], 0.9):
                    space_with_QID[i[-1]] = i[:-1] + [res]
                    i = ""
                    break
            if len(i) > 0:
                still_no_match_list.append(i)
    print("space_with_QID", space_with_QID)
    return still_no_match_list, space_with_QID


def compare_coordinates_with_threhold(coordinate_wiki, lat_usr, long_usr, threshold):
    if (abs(float(coordinate_wiki[0]) - float(long_usr)) <= threshold) and (
        abs(float(coordinate_wiki[1]) - float(lat_usr)) <= threshold
    ):
        return True
    return False


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
        # Note(QG): this can be easily extended into a longer list to increase the possibility of matching. Only
        # return the first one now due to efficiency.
        return results[0]


def get_coordinate_from_wikidata(q):
    """
    A function to extract coordinate location (if exists) of a wikidata entity
    :param qname: a wikidata id
    :return: a list with tuples, each tuple is a (lat, long) combination
    """
    coordinate_list = []
    headers = {"User-Agent": "wikidatalookup/1.0.0"}
    with requests.Session() as s:
        response = s.get(
            URL,
            params={
                "format": "json",
                "query": QUERY_COORDINATE.format(q),
            },
            headers=headers,
        )
        if response.status_code == 200:  # a successful response
            results = response.json().get("results", {}).get("bindings")
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


def chunks(it, size):
    it = iter(it)
    return iter(lambda: tuple(islice(it, size)), ())


if __name__ == "__main__":

    # # To compare the extracting coordinate location with the info in Space.csv
    # space_url = (
    #     "https://raw.githubusercontent.com/readchina/ReadAct/2.0-RC-patch/csv/data/Space.csv"
    # )
    # geo_code_dict = read_space_csv(space_url)
    #
    # # To filter CSV entries with comparing to openstreetmap first
    # no_match_list = compare_to_openstreetmap(geo_code_dict)

    # # To compare the rest with wikidata info
    # all_still_no_match_list = []
    # dictionary_list = []
    # for chunk in chunks(no_match_list, 30):  # the digit here controls the batch size
    #     if len(chunk) > 0:
    #         l, d = geo_code_compare(chunk)
    #         if l is not None:
    #             all_still_no_match_list += l
    #         dictionary_list += [d]
    #         print("\n I am taking a break XD \n")
    #         time.sleep(
    #             10
    #         )  # for every a few  entries, let this script take a break of 90 seconds
    # print("Finished the whole iteration")
    # print(all_still_no_match_list)
    # print("dictionary_list", dictionary_list)
    # match_for_space = {k: v for x in dictionary_list for k, v in dict(x).items()}
    # print(match_for_space)
    # with open("../results/match_for_space.json", "w") as f:
    #     json.dump(match_for_space, f)

    """
    The following is a one-time script to deactivate the compare_to_openstreetmap function to get as much Q-ids as
    we can.
    """

    # To compare the extracting coordinate location with the info in Space.csv
    space_url = "https://raw.githubusercontent.com/readchina/ReadAct/2.0-RC-patch/csv/data/Space.csv"
    geo_code_dict = read_space_csv(space_url)

    # Assume 0 match from openstreetmap
    no_match_list = []
    for k, v in geo_code_dict.items():
        if v[0] != "unknown" and v[2] != 0.0:
            item = v + [k]
            no_match_list.append(item)

    # To compare the rest with wikidata info
    all_still_no_match_list = []
    dictionary_list = []
    for chunk in chunks(no_match_list, 30):  # the digit here controls the batch size
        if len(chunk) > 0:
            l, d = geo_code_compare(chunk)
            if l is not None:
                all_still_no_match_list += l
            dictionary_list += [d]

            print("\n I am taking a break XD \n")
            time.sleep(
                10
            )  # for every a few  entries, let this script take a break of 90 seconds

    print("Finished the whole iteration")
    print("all_still_no_match_list:", all_still_no_match_list)
    print("dictionary_list", dictionary_list)
    match_for_space = {k: v for x in dictionary_list for k, v in dict(x).items()}
    print(match_for_space)
    with open("../results/match_for_space_without_Openstreetmap.json", "w") as f:
        json.dump(match_for_space, f)
