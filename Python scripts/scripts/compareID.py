import collections
import json
import re

from textblob import TextBlob
from lxml import etree, builder
from extractWikidataProperties import search_wikidata_id_by_querying, search_wikidata_id_by_wikibaseintegrator_in_Chinese


def readXML(xml_file=None):
    """
    A function to read the xml file and return a list.
    Empty elements are excluded by checking if its text is "None".
    :param xml_file: the xml file to read.
    :return: A list that its elements are all tuples. Tuple: (entity name, entity id in xml).
    """
    parser = etree.XMLParser()
    tree = etree.parse(xml_file, parser)
    root = tree.getroot()

    entity_list = []
    for element in root.iterdescendants():
        nodename = etree.QName(element)
        parent = element.getparent()
        if (
                nodename.localname == "placeName" or nodename.localname == "persName" or nodename.localname == "orgName") and element.text is not None:
            id_in_xml = parent.attrib.values()[0]
            entity_list.append((element.text, id_in_xml))
    return entity_list


def compareID(tuple_list=None):
    """
    The idea is, dictionary only has unique keys. If there are multiple elements in the value (which is a list),
    then we are sure that those entities who share the same key are identical.
    For entity names which will be searched in "ch", all the whitespaces before/in/after names are removed (for example, "林 立果" -> "林立果"）.
    :param tuple_list:
    :return: a dictionary. The key is the result of querying the entity name on wiki api,
    the value is a list of tuples - (entity name, entity id in standOff).
    """
    d = dict()
    for entity in tuple_list:
        entity_name = entity[0]
        # Todo: this regex search is not entirely correct with distinguish the language of Named Entity
        if re.search('[a-zA-Z]', entity_name) is not None:
            id_via_API = search_wikidata_id_by_querying(entity_name)
        else:
            id_via_API = search_wikidata_id_by_wikibaseintegrator_in_Chinese(entity_name.replace(" ", ""))

        print(entity)
        print("id_via_API: ", id_via_API)
        if id_via_API in d.keys():
            d[id_via_API].append([entity])
        else:
            d[id_via_API] = [entity]
    return collections.OrderedDict(sorted(d.items()))


def write_into_xml(dictionary=None):
    dictionary.pop('Not in database', None)
    E = builder.ElementMaker()
    root = E.root
    doc = E.doc
    # Todo: write the dictionary elements with valid wikidata id into a xml file,
    # so that a new XQuery script can read it and use it to decide if any entities should be merged.


if __name__ == "__main__":
    filename = "standOff.xml"
    aList = readXML(filename)
    dictionary = compareID(aList)
    with open('dictionary_with_API_ids.json', 'w') as f:
        json.dump(dictionary, f)

    # Todo: To call write_into_xml() here.

    # Todo: add something to query VIAF as well, and combine the result with query wikidata together.
