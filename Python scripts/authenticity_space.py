"""
This is a python script to check authenticity of Named Entities in /Readact/csv/data and in SCB.
- Get lookups from ReadAct
- Use openstreetmap API to filter those matched ones
- Use SPARQL to check the rest:
    - Get Q-identifiers for a lookup
    - retrieve the properties of coordinates from wikidata
    - Compare the retrieved coordinate with the coordinate stored in ReadAct
"""

import csv
import requests
import pandas as pd
from wikibaseintegrator import wbi_core
from wikidataintegrator import wdi_core


def read_space_csv(space_url):
    """
    A function to read "Space.csv" for now.
    :param filename: "Space.csv" for now
    :return: a dictionary of coordinate locations
    """
    df = pd.read_csv(space_url, error_bad_lines=False)
    geo_code_dict = {}
    for index, row in df.iterrows():
        if row['space_type'] != "L" and row['space_name'] != "unknown":
            # consider the case that if there are identical space_names in csv file
            if row['space_id'] not in geo_code_dict:
                # key: string: unique space_id
                # value: list: space_name, lat, lang
                geo_code_dict[row['space_id']] = [row['space_name'], row['lat'], row['long']]
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
        lat = str(v[1])
        lon = str(v[2])
        url = "https://nominatim.openstreetmap.org/reverse?format=xml&lat=" + lat + "&lon=" + lon + \
              "&zoom=18&addressdetails=1&format=json&accept-language=en"
        data = requests.get(url)
        if v[0].lower() not in str(data.json()).lower():
            no_match_list.append(v)
    return no_match_list


def geo_code_compare(no_match_list):
    """
    For geo locations in Space.csv, compare latitude/longitude for matching via retrieve data from wikidata.
    :param geo_code_dict: key: unique (lat,long) tuples; value: space_name in csv
    :return: None or list of entries which can't match
    """
    still_no_match_list = []
    break_out_flag = False
    for i in no_match_list:
        q_ids = _get_q_ids(i[0])
        # if no q_ids, collect item into list, break current loop
        if q_ids is None:
            still_no_match_list.append(i)
        else:
            coordinate_list = _get_coordinate_from_wikidata(q_ids)
            print("coordinate_list: ", coordinate_list)
            # if no coordinate_list, collect item into list, break nested loop
            if coordinate_list is None:
                still_no_match_list.append(i)
                break
            for i_wiki in coordinate_list:
                # If the difference are within +-0.9, consider a match, no collection, break nested loop
                if (float(abs(i_wiki[0])) - 0.9 <= float(i[1]) <= float(abs(i_wiki[0])) + 0.9) and \
                        (float(abs(i_wiki[1])) - 0.9 <= float(i[2]) <= float(abs(i_wiki[1])) + 0.9):
                    i = ""
                    break
            if len(i) > 0:
                still_no_match_list.append(i)

    if len(still_no_match_list) != 0:
        return still_no_match_list


def _get_q_ids(lookup=None):
    """
    A function to search qnames in wikidata with a lookup string.
    :param lookup: a string
    :return: a list of item identifiers (all)
    """
    e = wbi_core.FunctionsEngine()
    instance = e.get_search_results(search_string=lookup,
                                    search_type='item')

    if len(instance) > 0:
        # Speed up with less accuracy, use:
        return instance[0:1]
        # return instance
    else:
        print("Lookup not in database")
        return None


def _get_coordinate_from_wikidata(q_ids):
    """
    A function to extract coordinate location(if exists) of a wikidata entity
    :param qname: a list of Qname
    :return: a list with tuples, each tuple is a (lat, long) combination
    """
    coordinate_list = []
    if q_ids is None:
        return None
    for q in q_ids:
        wdi = wdi_core.WDItemEngine(wd_item_id=q)
        # to check successful installation and retrieval of the data, one can print the json representation of the item
        data = wdi.get_wd_json_representation()

        if "P625" in data["claims"]:
            # Iteration, in case one wikidata entity has several coordinate entries.
            for element in data["claims"]["P625"]:
                coordinate_value = element['mainsnak']['datavalue']['value']
                coordinate_list.append((coordinate_value['latitude'], coordinate_value['longitude']))
    return coordinate_list


if __name__ == "__main__":
    # To compare the extracting coordinate location with the info in Space.csv
    space_url = "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Space.csv"
    geo_code_dict = read_space_csv(space_url)

    # To filter CSV entries with comparing to openstreetmap first
    no_match_list = compare_to_openstreetmap(geo_code_dict)

    # To compare the rest with wikidata info
    still_no_match_list = geo_code_compare(no_match_list)

    print("still_no_match_list: ", still_no_match_list)

"""
still_no_match_list:  [['Bolshoy Fontan', 46.482526, 30.723309999999998], ['Vonu', 40.141308, 19.692947], ['Beidahuang', 45.73722, 126.69244099999999], ['Jinjiang (Fujian)', 24.781681, 118.552365], ['Gobi Desert', 42.795154, 105.03236299999999], ['Luoyang', 23.16244, 114.27342], ['Saratow', 51.592365, 45.960803999999996], ['Huangbei', 29.758889, 118.534167], ['Yizhen', 34.203246, 108.94589599999999], ['Xixian', 32.342791999999996, 114.74045600000001], ['Shanghexi', 39.4065, 112.9054], ['Chadian', 39.262324, 117.80593200000001], ['St. Louis', 38.627003, -90.199404], ['Zhongxian', 30.355947999999998, 107.83845], ['Suibin Nongchang', 47.523305, 131.69029], ['Viliya', 50.193612, 26.260521999999998], ['Fengshan', 41.208899, 116.645932], ['Wanxian', 30.807667, 108.40866100000001], ['Osino-Gay', 53.03739100000001, 42.402225], ['Zhanhai', 29.95481, 121.70961000000001], ['Xiangchuan', 28.515646000000004, 112.134533], ['Washington D.C.', 38.907191999999995, -77.03687099999999], ['Hannibal', 36.151664000000004, -95.991926], ['Groot-Zundert', 51.469834000000006, 4.654992], ['Zima (Siberia)', 53.922585, 102.042387], ['Strelkovka', 55.002389, 36731.0], ['Gudalovka', 49.307427000000004, 19.937017], ['St. Thomas', 18.338096, -64.894095], ['Albany NY', 42.652578999999996, -73.756232], ['Jiangxi Province', 27.285970000000002, 116.01608999999999], ['Friend', 40.6536, 97.2862], ['Marbach am Necker', 48.9396, 9.2646], ['Milan (OH)', 41.293333000000004, -82.601389], ['Jianyang', 30.24, 104.32], ['Chuansha Xian', 31.301395, 121.51665200000001], ['Sichuan Second Prison', 29.589209999999998, 106.538559], ['Laoting', 22.88778, 120.46356000000002], ['Shuiyuan county', 23.84967, 110.40083], ['Hubei', 37.59857, 114.60758], ['Banzai', 25.92448, 118.27899], ['San Fransisco', 37.774929, -122.419418], ['Wanzai', 22.913870000000003, 120.33538], ['Rugao', 22.74024, 120.49042], ['Tschita', 52.03861, 113.50425], ['Gerasimovka', 52.70488, 51.50281], ['Коsа (Kosinsky District)', 59.94537, 54.99187], ['Pucheng (Shaanxi)', 34.957, 109.58], ['Lliulin (Shanxi)', 37.430833, 110.88916699999999], ['Xiaxian', 35.138333, 111.22083300000001], ['Sora (Lazio)', 41.71667, 13.6176], ['Jiner', 31.6153, 107.654], ['Lixian', 29.631807000000002, 111.76076200000001]]
"""
