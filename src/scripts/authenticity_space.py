"""
This is a python script to check authenticity of Named Entities in /Readact/csv/CSV and in SCB.
- Get lookups from ReadAct
- Use openstreetmap API to filter those matched ones
- Use SPARQL to check the rest:
    - Get Q-identifiers for a lookup
    - retrieve the properties of coordinates from wikidata
    - Compare the retrieved coordinate with the coordinate stored in ReadAct
"""

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
            res = get_QID(i[0])
            print("res: ", res)

        if count == 20:
            time.sleep(30)
            count = 0

        if res is None:
            still_no_match_list.append(i)
        else:
            coordinate_list = get_coordinate_from_wikidata(res["id"])
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
        # print("still_no_match_list: ", still_no_match_list)
        return still_no_match_list


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
        # Note(QG): this can be easily extended into a longer list to increase the possibility of matching. Only return the first one now due to efficiency.
        return results[0]


def get_coordinate_from_wikidata(q):
    """
    A function to extract coordinate location(if exists) of a wikidata entity
    :param qname: a list of Qname
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
        # print("==>", response.status_code)
        if response.status_code != 200:
            print("===============")
        if response.status_code == 200:  # a successful response
            results = response.json().get("results", {}).get("bindings")

            # print(results)
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
    d = {"SP0025": ["Baiyangdian", "PL", 38.941441, 115.969465]}
    print(
        compare_to_openstreetmap(
            {"SP0025": ["Baiyangdian", "PL", 38.941441, 115.969465]}
        )
    )

    # # To compare the extracting coordinate location with the info in Space.csv
    # space_url = (
    #     "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Space.csv"
    # )
    # geo_code_dict = read_space_csv(space_url)
    #
    # # To filter CSV entries with comparing to openstreetmap first
    # no_match_list = compare_to_openstreetmap(geo_code_dict)
    #
    # # no_match_list = [
    # #     ["Hongkong", "PL", 22.396428, 114.109497],
    # #     ["Baiyangdian", "PL", 38.941441, 115.969465],
    # #     ["Breslau", "PL", 51.107885, 17.038538],
    # #     ["Sheker", "PL", 42.544292, 71.171379],
    # #     ["Bolshoy Fontan", "PL", 46.482526, 30.72331],
    # #     ["Birmendreïs", "PL", 36.735349, 3.050374],
    # #     ["Vonu", "PL", 40.141308, 19.692947],
    # #     ["Sveaborg", "PL", 60.1454, 24.98814],
    # #     ["Beidahuang", "PL", 45.73722, 126.692441],
    # #     ["Urumqi", "PL", 43.825592, 87.616848],
    # #     ["Jinjiang (Fujian)", "PL", 24.781681, 118.552365],
    # #     ["Lufeng", "PL", 23.165614, 116.210632],
    # #     ["Ningbo", "PL", 29.868336, 121.54399],
    # #     ["Dréan", "PL", 36.6848, 7.7511],
    # #     ["Gobi Desert", "PL", 42.795154, 105.032363],
    # #     ["Luoyang", "PL", 23.16244, 114.27342],
    # #     ["Saratow", "PL", 51.592365, 45.960804],
    # #     ["Huangbei", "PL", 29.758889, 118.534167],
    # #     ["Hankou", "PL", 30.541831166, 114.32583203],
    # #     ["Hangzhou", "PL", 29.9978, 119.7722],
    # #     ["Düsseldorf", "PL", 51.227741, 6.773456],
    # #     ["Yizhen", "PL", 34.203246, 108.945896],
    # #     ["Xixian", "PL", 32.342792, 114.740456],
    # #     ["Kalinovka", "PL", 51.893853, 34.509259],
    # #     ["Kislowodsk", "PL", 43.905601, 42.728095],
    # #     ["Xingtai", "PL", 37.070834, 114.504677],
    # #     ["Shanghexi", "PL", 39.4065, 112.9054],
    # #     ["Chaocheng", "PL", 36.05627, 115.590164],
    # #     ["Xibaipo", "PL", 38.351264, 113.940554],
    # #     ["Chadian", "PL", 39.262324, 117.805932],
    # #     ["Kiev", "PL", 50.4501, 30.5234],
    # #     ["St. Louis", "PL", 38.627003, -90.199404],
    # #     ["Saint Denis", "PL", 48.936181, 2.357443],
    # #     ["Zhongxian", "PL", 30.355948, 107.83845],
    # #     ["Jiutai", "PL", 44.135246, 125.977127],
    # #     ["Suibin Nongchang", "PL", 47.523305, 131.69029],
    # #     ["Viliya", "PL", 50.193612, 26.260522],
    # #     ["Fengshan", "PL", 41.208899, 116.645932],
    # #     ["Wanxian", "PL", 30.807667, 108.408661],
    # #     ["Osino-Gay", "PL", 53.037391, 42.402225],
    # #     ["Ji’an", "PL", 27.0875, 114.9645],
    # #     ["Zhanhai", "PL", 29.95481, 121.70961],
    # #     ["Xiangchuan", "PL", 28.515646, 112.134533],
    # #     ["Yasnaya Polyana", "PL", 54.069504, 37.523205],
    # #     ["Welyki Sorotschynzi", "PL", 50.019808, 33.941673],
    # #     ["Washington D.C.", "PL", 38.907192, -77.036871],
    # #     ["Calcutta", "PL", 22.572646, 88.363895],
    # #     ["Hannibal", "PL", 36.151664, -95.991926],
    # #     ["Groot-Zundert", "PL", 51.469834, 4.654992],
    # #     ["Trmanje", "PL", 42.647545, 19.344489],
    # #     ["Zima (Siberia)", "PL", 53.922585, 102.042387],
    # #     ["Strelkovka", "PL", 55.002389, 36731.0],
    # #     ["Gudalovka", "PL", 49.307427, 19.937017],
    # #     ["St. Thomas", "PL", 18.338096, -64.894095],
    # #     ["Albany NY", "PL", 42.652579, -73.756232],
    # #     ["Chuguyev", "PL", 49.836316, 36.681312],
    # #     ["Slawno", "PL", 54.36262, 16.67836],
    # #     ["Zavosse", "PL", 53.289514, 26.099846],
    # #     ["Jiangxi Province", "PL", 27.28597, 116.01609],
    # #     ["Chicago", "PL", 41.8781, 87.6298],
    # #     ["Vyoshenskaya", "PL", 49.6316, 41.7147],
    # #     ["Haining", "PL", 30.5107, 120.6808],
    # #     ["Salinas", "PL", 36.6777, 121.6555],
    # #     ["Friend", "PL", 40.6536, 97.2862],
    # #     ["Marbach am Necker", "PL", 48.9396, 9.2646],
    # #     ["Milan (OH)", "PL", 41.293333, -82.601389],
    # #     ["Jianyang", "PL", 30.24, 104.32],
    # #     ["Chuansha Xian", "PL", 31.301395, 121.516652],
    # #     ["Sichuan Second Prison", "PL", 29.58921, 106.538559],
    # #     ["Milano", "PL", 45.46362, 9.18812],
    # #     ["Laoting", "PL", 22.88778, 120.46356],
    # #     ["Shuiyuan county", "PL", 23.84967, 110.40083],
    # #     ["Hubei", "PL", 37.59857, 114.60758],
    # #     ["Warszawa", "PL", 52.229675, 21.01223],
    # #     ["Salamis Island", "PL", 37.96421, 23.49645],
    # #     ["Eleusis", "PL", 38.043228, 23.54212],
    # #     ["Banzai", "PL", 25.92448, 118.27899],
    # #     ["San Fransisco", "PL", 37.774929, -122.419418],
    # #     ["Wanzai", "PL", 22.91387, 120.33538],
    # #     ["Rugao", "PL", 22.74024, 120.49042],
    # #     ["Tschita", "PL", 52.03861, 113.50425],
    # #     ["Gerasimovka", "PL", 52.70488, 51.50281],
    # #     ["Коsа (Kosinsky District)", "PL", 59.94537, 54.99187],
    # #     ["Pucheng (Shaanxi)", "PL", 34.957, 109.58],
    # #     ["Lliulin (Shanxi)", "PL", 37.430833, 110.889167],
    # #     ["Xiaxian", "PL", 35.138333, 111.220833],
    # #     ["Gao’an", "PL", 28.441, 115.361],
    # #     ["Sora (Lazio)", "PL", 41.71667, 13.6176],
    # #     ["Huai’an", "PL", 33.555605, 119.112818],
    # #     ["Jiner", "PL", 31.6153, 107.654],
    # #     ["Wu’an", "PL", 36.698, 114.203],
    # #     ["Rui’an", "PL", 27.783333, 120.625],
    # #     ["Pilsen", "PL", 49.746841, 13.37699],
    # #     ["Tian'anmen Square", "L", 39.9042, 116.407396],
    # #     ["Three Gorges Dam", "L", 30.81329, 111.014292],
    # # ]
    #
    # print(len(no_match_list))
    #
    # # To compare the rest with wikidata info
    # all_still_no_match_list = []
    # for chunk in chunks(no_match_list, 30):  # the digit here controls the batch size
    #     if len(chunk) > 0:
    #         l = geo_code_compare(chunk)
    #         if l is not None:
    #             all_still_no_match_list += l
    #         print("\n I am taking a break XD \n")
    #         time.sleep(
    #             10
    #         )  # for every a few  entries, let this script take a break of 90 seconds
    # print("Finished the whole iteration")
    # print(all_still_no_match_list)
