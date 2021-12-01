"""
This is a python script to check authenticity of Named Entities in /Readact/csv/data and in SCB.
Main idea:
- Read ReadAct CSV files, get names as lookups
- Get the Q-identifier with library wikibaseintegrator for each lookup
- Use SPARQL to retrieve the property we need
- Compare wikidata item properties with data in ReadAct
"""
import pandas as pd
import requests
import time
from authenticity_space import read_space_csv
from itertools import islice
import json
import requests

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
################## Approach 1 : Query by name ##################
def read_person_csv(person_url="https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Person.csv"):
    """
    A function to read "Person.csv", preprocess data.
    :param filename: "Person.csv".
    :return: a dictionary: key: (person_id, name_lang); value: [name ordered,sex,birthyear, deathyear,
    altname, place of birth]
    """
    df = pd.read_csv(person_url, error_bad_lines=False)
    person_dict = {}
    place_dict = read_space_csv()
    for index, row in df.iterrows():
        id = row['person_id']
        print("-----\n", index, id)
        # a dictionary to collect final q_id for each unique person id
        if id not in person_dict:
            person_dict[id] = dict()

        if row['name_lang'] not in person_dict[row['person_id']]:
            # name_ordered is a list of a single name or multiple names
            name_ordered = __order_name_by_language(row)

        # sex or gender type in Wikidata for human: male, female, non-binary, intersex, transgender female,
        # transgender male, agender.
        if row['sex'] not in ['male', 'female', 'non-binary', 'intersex',
                              'transgender female', 'transgender ','male', 'agender']:
            row['sex'] = ""
        else:
            row['sex'] = row['sex'].strip()

        # birth_years and death_years are two lists of a single year or multiple years
        birth_years = __clean_birth_death_year_format(row['birthyear'])
        death_years = __clean_birth_death_year_format(row['deathyear'])

        if type(row['alt_name']) != str:
            row['alt_name'] = ""
        else:
            row['alt_name'] = row['alt_name'].strip()

        # Replace space_id with the name of space
        if row['place_of_birth'] in place_dict:
            row['place_of_birth'] = place_dict[row['place_of_birth']][0]
        else:
            print("Please check why the place of birth is not in the dictionary of space.")

        person_dict[id][row['name_lang']] = [name_ordered, row['sex'], birth_years, death_years, row['alt_name'],
        row['place_of_birth']]
    return person_dict


def __order_name_by_language(row):
    if type(row['family_name']) != str:
        name_ordered = [row['first_name']]
    elif type(row['first_name']) != str:
        name_ordered = [row['family_name']]
    elif row['name_lang'] == 'zh':
        name_ordered = [row['family_name'] + row['first_name']]  # 毛泽东
    else:
        # Make it a list with two types of order to suit different languages
        # Since the non-Chinese names are the minority, the influence on processing speed can be tolerant
        # For example:
        # pinyin name: family name + " " + first name
        # Korean name: family name + " " + first name
        # Russian name: first name + " " + family name
        name_ordered = [row['first_name'] + " " + row['family_name'], row['family_name'] + " " + row['first_name']]
    return name_ordered


def __clean_birth_death_year_format(default_year):
    char_to_remove = ['[', ']', '?', '~']
    cleaned_year = default_year
    for c in char_to_remove:
        cleaned_year = cleaned_year.replace(c, "")

    if cleaned_year.isalpha():  # "XXXX" contain 0 information
        years = []
    elif '.' in cleaned_year:
        cleaned_year = cleaned_year.split('..')
        years = list(range(int(cleaned_year[0]), int(cleaned_year[1]) + 1))
    elif '-' in cleaned_year:  # For BCE year
        cleaned_year = int(cleaned_year.replace('-', ''))
        years = [cleaned_year + 1, cleaned_year, cleaned_year - 1]
    elif any([i.isalpha() for i in cleaned_year]):
        cleaned_year = [cleaned_year.replace('X', '0').replace('x', '0'),
                        cleaned_year.replace('X', '9').replace('x', '0')]
        # Maybe consider to tickle the weight at this step already? since range(1000,2000) covers 1000 years and it
        # does not offer really useful information
        years = list(range(int(cleaned_year[0]), int(cleaned_year[1]) + 1))
    elif ',' in cleaned_year:
        cleaned_year = cleaned_year.split(',')
        years = list(range(int(cleaned_year[0]), int(cleaned_year[1]) + 1))
    else:
        years = [int(cleaned_year)]
    return years


def get_person_weight(person_dict, sleep=2):
    person_weight_dict = {}
    for person_id, value in person_dict.items():
        name_langs = value.keys()
        l = []
        person_weight_dict[person_id] = []
        for lang in name_langs:
            v = value[lang]
            # Use the ordered_name list as lookups
            lookup_names = v[0]

            # If there are alt_name, add it into the list of names
            if len(value[lang][4]) != 0:
                lookup_names.append(v[4])

            # print("====lookup_names:\n", lookup_names)
            if lookup_names != ['anonymous'] and lookup_names != ['无名']:
                person = _sparql(lookup_names, lang, sleep)
            else:
                continue

            if len(person) == 0:
                # print("There is no match for the person unique id ", person_id, " with language ", lang)
                pass

            else:
                weight = 0
                weights = []
                Qids = []
                wiki = []
                for Q_id, p in person.items():
                    # all the matched fields will add weight 1 to the total weight for this Q_id
                    if 'gender' in p:
                        if p['gender'] == v[1]:
                            weight += 1
                    elif 'birthyear' in p:
                        if p['birthyear'] in v[2]:
                            weight += 1
                    elif 'deathyear' in p:
                        if p['deathyear'] in v[3]:
                            weight += 1
                    elif 'birthplace' in p:
                        if p['birthplace'] == v[5]:
                            weight += 1

                    weights.append(weight)
                    Qids.append(Q_id)
                    wiki.append(p)
                    weight = 0

                l.append(lang)
                l.append(weights)
                l.append(Qids)
                l.append(wiki)

            if len(l) > 0 :
                person_weight_dict[person_id].append(l)
            l = []
    return person_weight_dict


def _sparql(lookup_names, lang, sleep=2):
    if len(lookup_names) == 0:
        return None
    person = {} # To collect entities which is found for the same person with different names
    with requests.Session() as s:
        for lookup in lookup_names:
            # print("++++ For this name: \n", lookup)
            response = s.get(URL, params={"format": "json", "query": QUERY.format(lookup, lang, lookup, lang)})
            if response.status_code == 200:  # a successful response
                results = response.json().get("results", {}).get("bindings")
                if len(results) == 0:
                    pass
                    # print("Didn't find the entity with this name \"", lookup, "\" on Wikidata")
                else:
                    for r in results:
                        person_wiki = {}
                        # If this entity is not recorded in the person dictionary yet:
                        if r['person']['value'][31:] not in person:
                            if "person" in r:
                                person_wiki['Q-id'] = r['person']['value'][31:] # for example, 'Q558744'
                            if "personLabel" in r:
                                person_wiki['name'] = r['personLabel']['value']
                            if "genderLabel" in r:
                                person_wiki["gender"] = r['genderLabel']['value']
                            if "ybirth" in r:
                                person_wiki["birthyear"] = r['ybirth']['value']
                            if "ydeath" in r:
                                person_wiki["deathyear"] = r['ydeath']['value']
                            if "birthplaceLabel" in r:
                                person_wiki['birthplace'] = r['birthplaceLabel']['value']

                            person[person_wiki['Q-id']] = person_wiki
            time.sleep(sleep)
    return person

def compare_weights(person_weight_dict):
    no_match = []
    person_match_dict = {}
    for id, p in person_weight_dict.items():
        # No match at all
        if len(p) < 1 :
            no_match.append(id)
        # At least one match for one name_lang, no match for the rest name_lang
        elif len(p) == 1:
            lang = p[0][0]
            weights = p[0][1]
            max_weight = max(weights) # for multiple identical weights to be the max weight, return the first max weight
            index = weights.index(max_weight)
            Qid = p[0][2][index]
            wi = p[0][3][index]
            person_match_dict[id] = [Qid, wi]
        # There are at least one match for each name_lang
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
                    max_weight = max(weights) # for multiple identical weights to be the max weight, return the first
                    index = weights.index(max_weight)
                    Qid = item[2][index]
                    wiki = item[3][index]
                if max_weight > m:
                    m = max_weight
                    q = Qid
                    w = wiki

                person_match_dict[id] = [q, w]

    return no_match, person_match_dict


def chunks(person_dict, SIZE= 20):
    it = iter(person_dict)
    for i in range(0, len(person_dict), 2):
        yield {k:person_dict[k] for k in islice(it, SIZE)}


########################################################################
################## Approach 2 : query by Q-identifier ##################
def get_matched_by_wikipedia_url(person_url="https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Person.csv"):
    df = pd.read_csv(person_url, error_bad_lines=False)
    person_matched_by_wikipedia = {}

    count = 0
    for index, row in df.iterrows():
        id = row['person_id']
        print(index, id)
        if id not in person_matched_by_wikipedia:
            Qid = __get_Qid_from_wikipedia_url(row)
            print("Qid: ", Qid)
            if Qid is not None:
                count += 1
                if count == 10:
                    time.sleep(90)
                    count = 0
                wiki = __sparql_with_Qid(Qid)
                if len(wiki) > 0 :
                    person_matched_by_wikipedia[id] = [Qid, wiki]
    return person_matched_by_wikipedia


def __get_Qid_from_wikipedia_url(row):
    # link: the wikipedia link in Person.csv
    if isinstance(row['source_1'], str) and ".wikipedia.org/wiki/" in row['source_1']:
        link = row['source_1']
    elif isinstance(row['source_2'], str) and ".wikipedia.org/wiki/" in row['source_2']:
        link = row['source_2']
    else:
        return None
    if len(link) > 30:
        language = link[8:10]
        name = link[30:]
        # Use MediaWiki API to query
        url = "https://" + language + ".wikipedia.org/w/api.php?action=query&prop=pageprops&titles=" + name + "&format=json"
        response = requests.get(url).json()
        if 'pageprops' in list(response['query']['pages'].values())[0]:
            pageprops = list(response['query']['pages'].values())[0]['pageprops']
            if 'wikibase_item' in pageprops:
                Qid = list(response['query']['pages'].values())[0]['pageprops']['wikibase_item']
                return Qid


def __sparql_with_Qid(Qid):
    wiki_dict = {}
    with requests.Session() as s:
        response = s.get(URL, params={"format": "json", "query": QUERY_WITH_QID.format(Qid)})
        if response.status_code == 200:  # a successful response
            results = response.json().get("results", {}).get("bindings")
            if len(results) == 0:
                print("Didn't find the entity with this Q-identifier \"", Qid, "\" on Wikidata")
                return None
            else:
                for r in results:
                    if r is not None:
                        wiki_dict['Q-id'] = Qid
                        if "personLabel" in r:
                            wiki_dict['name'] = r['personLabel']['value']
                        if "genderLabel" in r:
                            wiki_dict["gender"] = r['genderLabel']['value']
                        if "ybirth" in r:
                            wiki_dict["birthyear"] = r['ybirth']['value']
                        if "ydeath" in r:
                            wiki_dict["deathyear"] = r['ydeath']['value']
                        if "birthplaceLabel" in r:
                            wiki_dict['birthplace'] = r['birthplaceLabel']['value']
    return wiki_dict


if __name__ == "__main__":
########################################################################
################## Approach 2 : query by Q-identifier ##################
    matched_by_wikipedia = get_matched_by_wikipedia_url("https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Person.csv")

    # with open('matched_by_wikipedia.json', 'w') as f:
    #     json.dump(matched_by_wikipedia, f)


#################################################################
################## Comparison ##################
    # with open('final_person_match_dict.json', 'r') as f:
    #     name = json.load(f)
    # with open('matched_by_wikipedia.json', 'r') as f:
    #     wikipedia = json.load(f)
    #
    # print(len(wikipedia))
    # print(len(name))
    #
    # difference_between_two_approaches = []
    # l = [x for x in wikipedia.keys() if x in name.keys()]
    #
    # print("number of intersection: ", len(l)) # 374
    # for key in l:
    #     if name[key] != wikipedia[key]:
    #         print("\nFor person with id ", key, ':')
    #         print("name[key]: ", "\n", name[key])
    #         print("wikipedia[key]: ", "\n", wikipedia[key], '\n')
    #         difference_between_two_approaches.append(key)




#################################################################
################## Approach 1 : Query by name ##################
    # person_dict = read_person_csv(
    #     "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Person.csv")
    # # print(person_dict)

    # # Break the entire dictionary into several chunks.
    # # So that we can add break sessions in between to avoid exceed the limitation of SPARQL query
    # final_no_match = []
    # final_person_match_dict = {}
    # for chunk in chunks(person_dict, 30): # the digit here controls the batch size
    #     if len(chunk) > 0:
    #         print("chunk: \n", chunk)
    #         person_weight_dict = get_person_weight(chunk, 2)
    #         no_match, person_match_dict = compare_weights(person_weight_dict, person_match_dict)
    #         if len(no_match) > 0:
    #             final_no_match = [*final_no_match, *no_match]
    #         if len(person_match_dict) > 0:
    #             final_person_match_dict = {**final_person_match_dict, **person_match_dict}
    #
    #         print("\n----------\n")
    #         print("no match: ", no_match)
    #         print("person_match_dict: ", person_match_dict)
    #
    #         print("\n===========================\n")
    #         print("Current final no match: ", final_no_match)
    #         print("Current final person_match_dict: ", final_person_match_dict)
    #         print("\n I am taking a break XD \n")
    #
    #         time.sleep(90) # for every a few person entries, let this script take a break of 90 seconds
    #
    # print("I finished all the iteration.")
    # print("\n===========================\n")
    # print("no match: ", final_no_match)
    # print("person_match_dict: ", final_person_match_dict)

    # with open('final_no_match.json', 'w') as f:
    #     json.dump(final_no_match, f)
    #
    # with open('final_person_match_dict.json', 'w') as f:
    #     json.dump(final_person_match_dict, f)


#################################################################
################## Some tests for Approach 1  ##################
    # sample_dict = {
    #     'AG0089': {'en': [['Konstantin Balmont', 'Balmont Konstantin'], 'male', [1876], [1942], '', 'Shuya'], 'ru': [['Константи́н ''Бальмо́нт','Бальмо́нт Константи́н'], 'male', [1876], [1942], '', 'Shuya'], 'zh': [['巴尔蒙特康斯坦丁'], 'male', [1876], [1942], '', 'Shuya']},
    #     'AG0090': {'en': [['Honoré de Balzac', 'Balzac Honoré de'], 'male', [1799], [1850], '', 'Tours'], 'zh': [['巴尔扎克奥诺雷·德'], 'male', [1799], [1850], '', 'Tours']},
    #     'AG0091': {'en': [['Charles Baudelaire', 'Baudelaire Charles'], 'male', [1821], [1867], '', 'Paris'], 'zh': [['波德莱尔夏尔'], 'male', [1821], [1867], '', 'Paris']},
    #     'AG0092': {'en': [['Samuel Beckett', 'Beckett Samuel'], 'male', [1906], [1989], '', 'Foxrock'],
    #             'zh': [['贝克特萨缪尔'], 'male', [1906], [1989], '', 'Foxrock']},
    #     'AG0097': {'en': [['Ruxie Bi', 'Bi Ruxie'], 'male', [], [], '', 'unknown'], 'zh': [['毕汝协'], 'male', [], [], '毕汝谐', 'unknown']},
    #     'AG0098': {'en': [['Zhilin Bian', 'Bian Zhilin'], 'male', [1910], [2000], '', 'Haimen'], 'zh': [['卞之琳'], 'male', [1910], [2000], '', 'Haimen']},
    #     'AG0511': {'en': [['Er Nie', 'Nie Er'], 'male', [1912], [1935], 'Nie Shouxin', 'Vinci'], 'zh': [['聂耳'], 'male', [1912], [1935], '聂守信', 'Vinci']}
    # }
    #
    # person_weight_dict = get_person_weight(sample_dict, 2)
    #
    # print(person_weight_dict)

    # sample_person_weight_dict = {
    #     'AG0089': [['en', [1], ['Q314498'], [{'Q-id': 'Q314498', 'name': 'Konstantin Balmont', 'gender': 'male', 'birthyear': '1867', 'deathyear': '1942', 'birthplace': 'Q50074884'}]]],
    #     'AG0090': [['en', [1], ['Q9711'], [{'Q-id': 'Q9711', 'name': 'Honoré de Balzac', 'gender': 'male', 'birthyear': '1799', 'deathyear': '1850', 'birthplace': 'Tours'}]]],
    #     'AG0091': [['en', [1, 1], ['Q501', 'Q481146'], [{'Q-id': 'Q501', 'name': 'Charles Baudelaire', 'gender': 'male', 'birthyear': '1821', 'deathyear': '1867', 'birthplace': 'Paris'}, {'Q-id': 'Q481146', 'name': 'Pascal Pia', 'gender': 'male', 'birthyear': '1903', 'deathyear': '1979', 'birthplace': '10th arrondissement of Paris'}]]]
    # }

    # no_match, person_match_dict = compare_weights(sample_person_weight_dict)
    # print("no match: ", no_match)
    # print("person_match_dict: ", person_match_dict)


