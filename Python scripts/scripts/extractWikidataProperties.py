#!/usr/bin/python3.8
# -*- coding: <utf-8> -*-

"""
 This is a script for extracting properties of a wikidata entity.
"""

import re
import requests
import sys
from wikibaseintegrator import wbi_core
from wikidata.client import Client


def search_wikidata_id_by_querying(lookup=None):
    """
    A function to search for the wikidata id by querying with mediawiki API.
    :param lookup: The entity we want to search for, like a person name or an organization name
    :return: a string which has the pattern combined by letter "Q" and digits
    """
    endpoint = "https://en.wikipedia.org/w/api.php?action=query&prop=pageprops&titles=" + lookup + "&format=json"
    r = requests.get(endpoint)
    data = r.text
    result = re.search(r'Q\d+', data)
    if result is None:
        return "Not in database"
    else:
        return result.group()


def search_wikidata_id_by_parsing(lookup=None):
    """
    A function to search for the wikidata id by parsing the page with request.
    :param lookup: The entity we want to search for, like a person name or an organization name
    :return: a string which has the pattern combined by letter "Q" and digits
    """
    params = {
        'action': 'parse',
        'page': lookup,
        'prop': 'text',
        'formatversion': 2
    }

    r = requests.get("https://en.wikipedia.org/w/api.php", params=params)
    data = r.text

    # Can also use regex to search for all the urls with a certain pattern directly
    wikidata_url = 'https://www.wikidata.org/wiki/'
    if data.find(wikidata_url) != -1:
        idx = data.index(wikidata_url) + 30
        result = re.match(r'Q[0-9]+', data[idx:idx + 20]).group()
        return result


def search_wikidata_id_by_wikibaseintegrator_in_Chinese(lookup=None):
    e = wbi_core.FunctionsEngine()
    instance = e.get_search_results(search_string=lookup,
                                    search_type='item',
                                    max_results=5, language='ch')
    if len(instance) > 1:
        return instance[0]
    else:
        return "Not in database"


def get_property(wikidata_id=None, property_id=None):
    """
    A function to use Wikidata client library to get the value of property.
    :param wikidata_id: an unique wikidata id start with "Q"
    :param property_id: a string starts with "P"
    :return: string or int, depends on the property
    """
    client = Client()
    entity = client.get(wikidata_id, load=True)
    print("Entity description: ", entity.description)
    year_of_found = client.get(property_id)
    return entity[year_of_found]


if __name__ == "__main__":

    print(search_wikidata_id_by_wikibaseintegrator_in_Chinese("上海"))
    """
    Usage:
    $ python3.8 extractWikidataProperties.py Peking_University P571
    """

    # # extract inception
    # if len(sys.argv) == 3:
    #     lookup = sys.argv[1]
    #     inception_id = sys.argv[2]
    #
    #     # lookup_id = search_wikidata_id_by_parsing(lookup)
    #     lookup_id = search_wikidata_id_by_querying(lookup)
    #
    #     year = get_property(lookup_id, inception_id)
    #     print("The found year: ", year)
