import re
import requests


def query_t(lookup=None):
    base_url = "http://www.viaf.org"
    authority = "/viaf/search?query=cql.any+=+\"" + lookup + "\"&maximumRecords=5&httpAccept=application/json"

    r = requests.get(base_url+authority)
    data = r.text
    t = re.search(r'(\d{4})-(\d{4})', data)
    print(t.group())


if __name__ == "__main__":
    lookup = "Lu Xun"
    query_t(lookup)
