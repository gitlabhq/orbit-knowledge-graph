import json
import sys
import urllib.parse
import urllib.request

S = "https://sonarcloud.io/api"


def get(path, query):
    url = f"{S}/{path}?{urllib.parse.urlencode(query)}"
    with urllib.request.urlopen(url, timeout=90) as response:
        return json.load(response)


project = sys.argv[1]
seen = {}
page = 1
while True:
    data = get("components/tree", {"component": project, "qualifiers": "FIL,UTS", "ps": 500, "p": page})
    for component in data["components"]:
        if component.get("path", "").endswith(".java"):
            seen[component["path"]] = component["qualifier"]
    if page * 500 >= data["paging"]["total"] or page >= 40:
        break
    page += 1
json.dump(seen, sys.stdout)
