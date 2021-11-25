"""
This is a python script to check authenticity of Named Entities in /Readact/csv/data and in SCB.
Main idea:
- Read ReadAct CSV files, get lookups
- Get the Q-identifier with library wikibaseintegrator for each lookup
- Use SPARQL to retrieve the property we need
- Compare wikidata item properties with data in ReadAct
"""
import pandas as pd
import requests
import time
from authenticity_space import read_space_csv

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


def read_person_csv(person_url="https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Person.csv"):
    """
    A function to read "Person.csv", preprocess data.
    :param filename: "Person.csv".
    :return: a dictionary: key: (person_id, name_lang); value: [name ordered,sex,birthyear, deathyear,
    altname, place of birth]
    """
    df = pd.read_csv(person_url, error_bad_lines=False)
    person_dict = {}
    person_dict_with_name_lang = {}
    place_dict = read_space_csv()
    for index, row in df.iterrows():
        # a dictionary to collect final q_id for each unique person id
        person_dict[row['person_id']] = ""

        key = (row['person_id'], row['name_lang'])
        if key not in person_dict_with_name_lang:

            # name_ordered is a list of a single name or multiple names
            name_ordered = __order_name_by_language(row)

            # sex or gender type in Wikidata for human: male, female, non-binary, intersex, transgender female,
            # transgender male, agender.
            if row['sex'] not in ['male', 'female', 'non-binary', 'intersex', 'transgender female', 'transgender ',
                                  'male', 'agender']:
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

            person_dict_with_name_lang[key] = [name_ordered, row['sex'], birth_years, death_years, row['alt_name'],
                                row['place_of_birth']]
        else:
            print("Please check why the key is repeated. ")
    return person_dict_with_name_lang, person_dict


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


def compare(person_dict_with_name_lang, person_dict, sleep=2):
    for k, v in person_dict_with_name_lang.items():
        # get the language of the name, to be used as language tag in the SPARQL query
        person_id = k[0] # k[0]: {k[1]: weight, q-ids}
        lang = k[1]

        # Use the ordered_name list as lookups
        lookup_names = v[0]

        # If there are alt_name, add it into the list of names
        if len(v[4]) != 0:
            lookup_names.append(v[4])

        # print("====lookup_names:\n", lookup_names)
        if lookup_names != ['anonymous'] and lookup_names != ['无名']:
            person = _sparql(lookup_names, lang, sleep)
        else:
            continue

        if len(person) == 0:
            print("There is no match for the person unique id ", id, " with language ", lang)

        else:
            weight = 0
            weight_Q_pairs = {}
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

                weight_Q_pairs[Q_id] = weight
                weight = 0
            if len(weight_Q_pairs) == 0:
                print("There is no match for the person unique id ", id, " with language ", lang)

            else:
                q_ids = []
                max_weight = max(weight_Q_pairs.values())

                for id, w in weight_Q_pairs.items():
                   if w == max_weight:
                       q_ids.append(id)
                if len(q_ids) == 1:
                    # print("--This person, ", k, v, "should be matched with this Wikidata entity", ids[0],
                    #       '\nand this entity has the following information: ', person[ids[0]])
                    # person_dict[person_id] = {lang: [weight, ]}
                    pass
                else:
                    # this part will be used to correct wrong entries for matched person in Person.csv
                    # therefore, there should be an algorithm to choose the matched one from all the entities which
                    # all has the hightest weight
                    # print("--HEY, multiple highest-weight match for a person!") # should be decomment later
                    pass

    return no_match


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


if __name__ == "__main__":
    person_dict_with_name_lang, person_dict = read_person_csv(
        "https://raw.githubusercontent.com/readchina/ReadAct/master/csv/data/Person.csv")

    # print(person_dict)

    sample_dict = { ('AG0009', 'zh'): [['北岛'], 'male', [1949], [], '赵振开', 'Beijing'],('AG0004', 'en'): [['Ke Mang',
                                                                                                         'Mang Ke'],'male', [1950], [], 'Jiang Shiwei', 'Shenyang'], ('AG0004', 'zh'): [['芒克'], 'male', [1950], [], '姜世伟', 'Shenyang'], ('AG0005', 'en'): [['Gang Peng', 'Peng Gang'], 'male', [1952], [], '', 'Beijing'],('AG0009', 'en'): [['Dao Bei', 'Bei Dao'], 'male', [1949], [], 'Zhao Zhenkai', 'Beijing']}

    no_match = compare(person_dict_with_name_lang, person_dict, 2)
    print("no_match", no_match)
    print("-------length of the no_match", len(no_match))
