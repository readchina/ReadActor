"""
This is a python script to check authenticity of Named Entities in /Readact/csv/CSV and in SCB.
- Get lookups from ReadAct
- Use openstreetmap API to filter those matched ones
- Use SPARQL to check the rest:
    - Get Q-identifiers for a lookup
    - retrieve the properties of coordinates from wikidata
    - Compare the retrieved coordinate with the coordinate stored in ReadAct
"""
import pandas as pd
import requests

URL = "https://query.wikidata.org/sparql"

QUERY_SPACE = """
        SELECT distinct ?space ?spaceLabel
WHERE{{
    {{?space wdt:P31 wd:Q515;
                rdfs:label "{}"@{} . }}
    UNION
    {{?space wdt:P31 wd:Q3257686;
                rdfs:label "{}"@{} . }}
    UNION
    {{?space wdt:P31 wd:Q200250;
                rdfs:label "{}"@{} . }}
    UNION
    {{?space wdt:P31 wd:Q486972;
                rdfs:label "{}"@{} . }}
    UNION
    {{?space wdt:P31 wd:Q1180262;
                rdfs:label "{}"@{} . }}
    UNION
    {{?space wdt:P31 wd:Q1187811;
                rdfs:label "{}"@{} . }}
     UNION
    {{?space wdt:P31 wd:Q7930989;
                rdfs:label "{}"@{} . }}
     UNION
    {{?space wdt:P31 wd:Q134626;
                rdfs:label "{}"@{} . }}
     UNION
    {{?space wdt:P31 wd:Q515;
                skos:altLabel "{}"@{} . }}
    UNION
    {{?space wdt:P31 wd:Q3257686;
                skos:altLabel "{}"@{} . }}
    UNION
    {{?space wdt:P31 wd:Q200250;
                skos:altLabel "{}"@{} . }}
    UNION
    {{?space wdt:P31 wd:Q486972;
                skos:altLabel "{}"@{} . }}
    UNION
    {{?space wdt:P31 wd:Q1180262;
                skos:altLabel "{}"@{} . }}
    UNION
    {{?space wdt:P31 wd:Q1187811;
                skos:altLabel "{}"@{} . }}
     UNION
    {{?space wdt:P31 wd:Q7930989;
                skos:altLabel "{}"@{} . }}
     UNION
    {{?space wdt:P31 wd:Q134626;
                skos:altLabel "{}"@{} . }}
  
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language  "[AUTO_LANGUAGE], en"}}
}}
        LIMIT 250
        """

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
    for i in no_match_list:
        query_result = __sparql_by_space_name(i[0], "en")
        if query_result is None:
            still_no_match_list.append(i)
        else:
            q_ids = list(query_result.keys())
            coordinate_list = __get_coordinate_from_wikidata(q_ids)

            # if no coordinate_list, collect item into list, break nested loop
            if len(coordinate_list) == 0:
                still_no_match_list.append(i)
                break
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


def __sparql_by_space_name(lookup, lang):
    if len(lookup) == 0:
        return None
    space = {}  # To collect entities which is found for a space/location
    with requests.Session() as s:
        response = s.get(
            URL,
            params={
                "format": "json",
                "query": QUERY_SPACE.format(
                    lookup,
                    lang,
                    lookup,
                    lang,
                    lookup,
                    lang,
                    lookup,
                    lang,
                    lookup,
                    lang,
                    lookup,
                    lang,
                    lookup,
                    lang,
                    lookup,
                    lang,
                    lookup,
                    lang,
                    lookup,
                    lang,
                    lookup,
                    lang,
                    lookup,
                    lang,
                    lookup,
                    lang,
                    lookup,
                    lang,
                    lookup,
                    lang,
                    lookup,
                    lang,
                ),
            },
        )
        if response.status_code == 200:  # a successful response
            results = response.json().get("results", {}).get("bindings")
            if len(results) == 0:
                pass
            else:
                for r in results:
                    space_wiki = {}
                    # If this entity is not recorded in this space_wiki dictionary yet:
                    if "space" in r:
                        if r["space"]["value"][31:] not in space:
                            space_wiki["Q-id"] = r["space"]["value"][
                                31:
                            ]  # for example, 'Q8646'
                        if "spaceLabel" in r:
                            space_wiki["name"] = r["spaceLabel"]["value"]
                    space[space_wiki["Q-id"]] = space_wiki
    if len(space) == 0:
        return None
    else:
        print("Lookup not in database")
        return None
    else:
        return space


def __get_coordinate_from_wikidata(q_ids):
    """
    A function to extract coordinate location(if exists) of a wikidata entity
    :param qname: a list of Qname
    :return: a list with tuples, each tuple is a (lat, long) combination
    """
    coordinate_list = []
    for q in q_ids:
        with requests.Session() as s:
            response = s.get(
                URL,
                params={
                    "format": "json",
                    "query": QUERY_COORDINATE.format(q),
                },
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
                                coordinate_list.append(
                                    c
                                )  # for example, '[114.158611111,22.278333333]'
    return coordinate_list


if __name__ == "__main__":
    # To compare the extracting coordinate location with the info in Space.csv
    space_url = (
        "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Space.csv"
    )
    geo_code_dict = read_space_csv(space_url)

    # To filter CSV entries with comparing to openstreetmap first
    no_match_list = compare_to_openstreetmap(geo_code_dict)

    # To compare the rest with wikidata info
    still_no_match_list = geo_code_compare(no_match_list)

"""
still_no_match_list (with using the python library):  [['Bolshoy Fontan', 'PL', 46.482526, 30.723309999999998], 
['Vonu', 
'PL', 40.141308, 
19.692947], 
['Beidahuang', 'PL', 45.73722, 126.69244099999999], ['Jinjiang (Fujian)', 'PL', 24.781681, 118.552365], 
['Gobi Desert', 'PL', 42.795154, 105.03236299999999], ['Luoyang', 'PL', 23.16244, 114.27342], ['Saratow', 'PL', 
51.592365, 45.960803999999996], ['Huangbei', 'PL', 29.758889, 118.534167], ['Yizhen', 'PL', 34.203246, 
108.94589599999999], ['Xixian', 'PL', 32.342791999999996, 114.74045600000001], ['Shanghexi', 'PL', 39.4065, 
112.9054], ['Chadian', 'PL', 39.262324, 117.80593200000001], ['St. Louis', 'PL', 38.627003, -90.199404], 
['Zhongxian', 'PL', 30.355947999999998, 107.83845], ['Suibin Nongchang', 'PL', 47.523305, 131.69029], ['Viliya', 
'PL', 50.193612, 26.260521999999998], ['Fengshan', 'PL', 41.208899, 116.645932], ['Wanxian', 'PL', 30.807667, 
108.40866100000001], ['Osino-Gay', 'PL', 53.03739100000001, 42.402225], ['Zhanhai', 'PL', 29.95481, 
121.70961000000001], ['Xiangchuan', 'PL', 28.515646000000004, 112.134533], ['Washington D.C.', 'PL', 
38.907191999999995, -77.03687099999999], ['Hannibal', 'PL', 36.151664000000004, -95.991926], ['Groot-Zundert', 'PL', 
51.469834000000006, 4.654992], ['Zima (Siberia)', 'PL', 53.922585, 102.042387], ['Strelkovka', 'PL', 55.002389, 
36731.0], ['Gudalovka', 'PL', 49.307427000000004, 19.937017], ['St. Thomas', 'PL', 18.338096, -64.894095], 
['Albany NY', 'PL', 42.652578999999996, -73.756232], ['Jiangxi Province', 'PL', 27.285970000000002, 
116.01608999999999], ['Friend', 'PL', 40.6536, 97.2862], ['Marbach am Necker', 'PL', 48.9396, 9.2646], ['Milan (OH)', 
'PL', 41.293333000000004, -82.601389], ['Jianyang', 'PL', 30.24, 104.32], ['Chuansha Xian', 'PL', 31.301395, 
121.51665200000001], ['Sichuan Second Prison', 'PL', 29.589209999999998, 106.538559], ['Laoting', 'PL', 22.88778, 
120.46356000000002], ['Shuiyuan county', 'PL', 23.84967, 110.40083], ['Hubei', 'PL', 37.59857, 114.60758], ['Banzai', 
'PL', 25.92448, 118.27899], ['San Fransisco', 'PL', 37.774929, -122.419418], ['Wanzai', 'PL', 22.913870000000003, 
120.33538], ['Rugao', 'PL', 22.74024, 120.49042], ['Tschita', 'PL', 52.03861, 113.50425], ['Gerasimovka', 'PL', 
52.70488, 51.50281], ['Коsа (Kosinsky District)', 'PL', 59.94537, 54.99187], ['Pucheng (Shaanxi)', 'PL', 34.957, 
109.58], ['Lliulin (Shanxi)', 'PL', 37.430833, 110.88916699999999], ['Xiaxian', 'PL', 35.138333, 111.22083300000001], 
['Sora (Lazio)', 'PL', 41.71667, 13.6176], ['Jiner', 'PL', 31.6153, 107.654], ['Lixian', 'PL', 29.631807000000002, 
111.76076200000001]]
"""

"""
still_no_match_list (with using direct wikidata SPARQL service) :  [['Baiyangdian', 'PL', 38.941441, 115.969465], 
['Breslau', 'PL', 
51.107885, 17.038538], 
['Bolshoy Fontan', 'PL', 46.482526, 30.72331], ['Birmendreïs', 'PL', 36.735349, 3.050374], ['Vonu', 'PL', 40.141308, 
19.692947], ['Sveaborg', 'PL', 60.1454, 24.98814], ['Beidahuang', 'PL', 45.73722, 126.692441], ['Urumqi', 'PL', 
43.825592, 87.616848], ['Jinjiang (Fujian)', 'PL', 24.781681, 118.552365], ['Lufeng', 'PL', 23.165614, 116.210632], 
['Ningbo', 'PL', 29.868336, 121.54399], ['Thornton', 'PL', 53.7833, -1.85], ['Dréan', 'PL', 36.6848, 7.7511], 
['Gobi Desert', 'PL', 42.795154, 105.032363], ['Luoyang', 'PL', 23.16244, 114.27342], ['Saratow', 'PL', 51.592365, 
45.960804], ['Huangbei', 'PL', 29.758889, 118.534167], ['Hankou', 'PL', 30.541831166, 114.32583203], ['Düsseldorf', 
'PL', 51.227741, 6.773456], ['Yizhen', 'PL', 34.203246, 108.945896], ['Xixian', 'PL', 32.342792, 114.740456], 
['Kalinovka', 'PL', 51.893853, 34.509259], ['Kislowodsk', 'PL', 43.905601, 42.728095], ['Shanghexi', 'PL', 39.4065, 
112.9054], ['Chaocheng', 'PL', 36.05627, 115.590164], ['Xibaipo', 'PL', 38.351264, 113.940554], ['Chadian', 'PL', 
39.262324, 117.805932], ['Kiev', 'PL', 50.4501, 30.5234], ['St. Louis', 'PL', 38.627003, -90.199404], ['Saint Denis', 
'PL', 48.936181, 2.357443], ['Zhongxian', 'PL', 30.355948, 107.83845], ['Jiutai', 'PL', 44.135246, 125.977127], 
['Suibin Nongchang', 'PL', 47.523305, 131.69029], ['Viliya', 'PL', 50.193612, 26.260522], ['Fengshan', 'PL', 
41.208899, 116.645932], ['Wanxian', 'PL', 30.807667, 108.408661], ['Osino-Gay', 'PL', 53.037391, 42.402225], 
['Ji’an', 'PL', 27.0875, 114.9645], ['Zhanhai', 'PL', 29.95481, 121.70961], ['Xiangchuan', 'PL', 28.515646, 
112.134533], ['Yasnaya Polyana', 'PL', 54.069504, 37.523205], ['Welyki Sorotschynzi', 'PL', 50.019808, 33.941673], 
['Washington D.C.', 'PL', 38.907192, -77.036871], ['Calcutta', 'PL', 22.572646, 88.363895], ['Hannibal', 'PL', 
36.151664, -95.991926], ['Groot-Zundert', 'PL', 51.469834, 4.654992], ['Trmanje', 'PL', 42.647545, 19.344489], 
['Zima (Siberia)', 'PL', 53.922585, 102.042387], ['Strelkovka', 'PL', 55.002389, 36731.0], ['Gudalovka', 'PL', 
49.307427, 19.937017], ['St. Thomas', 'PL', 18.338096, -64.894095], ['Albany NY', 'PL', 42.652579, -73.756232], 
['Chuguyev', 'PL', 49.836316, 36.681312], ['Slawno', 'PL', 54.36262, 16.67836], ['Zavosse', 'PL', 53.289514, 
26.099846], ['Jiangxi Province', 'PL', 27.28597, 116.01609], ['Chicago', 'PL', 41.8781, 87.6298], ['Vyoshenskaya', 
'PL', 49.6316, 41.7147], ['Haining', 'PL', 30.5107, 120.6808], ['Salinas', 'PL', 36.6777, 121.6555], ['Friend', 'PL', 
40.6536, 97.2862], ['Marbach am Necker', 'PL', 48.9396, 9.2646], ['Milan (OH)', 'PL', 41.293333, -82.601389], 
['Jianyang', 'PL', 30.24, 104.32], ['Chuansha Xian', 'PL', 31.301395, 121.516652], ['Sichuan Second Prison', 'PL', 
29.58921, 106.538559], ['Laoting', 'PL', 22.88778, 120.46356], ['Shuiyuan county', 'PL', 23.84967, 110.40083], 
['Hubei', 'PL', 37.59857, 114.60758], ['Warszawa', 'PL', 52.229675, 21.01223], ['Salamis Island', 'PL', 37.96421, 23.49645], ['Eleusis', 'PL', 38.043228, 23.54212]]
"""


"""
still_no_match_list (with using direct wikidata SPARQL service, 2022-04-14) : 
[['Baiyangdian', 'PL', 38.941441, 115.969465], ['Breslau', 'PL', 51.107885, 17.038538], ['Bolshoy Fontan', 'PL', 46.482526, 30.72331], ['Birmendreïs', 'PL', 36.735349, 3.050374], ['Vonu', 'PL', 40.141308, 19.692947], ['Sveaborg', 'PL', 60.1454, 24.98814], ['Beidahuang', 'PL', 45.73722, 126.692441], ['Urumqi', 'PL', 43.825592, 87.616848], ['Jinjiang (Fujian)', 'PL', 24.781681, 118.552365], ['Lufeng', 'PL', 23.165614, 116.210632], ['Ningbo', 'PL', 29.868336, 121.54399], ['Thornton', 'PL', 53.7833, -1.85], ['Dréan', 'PL', 36.6848, 7.7511], ['Gobi Desert', 'PL', 42.795154, 105.032363], ['Luoyang', 'PL', 23.16244, 114.27342], ['Saratow', 'PL', 51.592365, 45.960804], ['Huangbei', 'PL', 29.758889, 118.534167], ['Hankou', 'PL', 30.541831166, 114.32583203], ['Düsseldorf', 'PL', 51.227741, 6.773456], ['Yizhen', 'PL', 34.203246, 108.945896], ['Xixian', 'PL', 32.342792, 114.740456], ['Kalinovka', 'PL', 51.893853, 34.509259], ['Kislowodsk', 'PL', 43.905601, 42.728095], ['Shanghexi', 'PL', 39.4065, 112.9054], ['Chaocheng', 'PL', 36.05627, 115.590164], ['Xibaipo', 'PL', 38.351264, 113.940554], ['Chadian', 'PL', 39.262324, 117.805932], ['Kiev', 'PL', 50.4501, 30.5234], ['St. Louis', 'PL', 38.627003, -90.199404]]
"""
