import json, sys, urllib.request, urllib.parse
S = "https://sonarcloud.io/api"
def get(path, **q):
    with urllib.request.urlopen(f"{S}/{path}?{urllib.parse.urlencode(q)}", timeout=60) as r:
        return json.load(r)
project, rules = sys.argv[1], sys.argv[2]
out = []
page = 1
while True:
    d = get("issues/search", componentKeys=project, rules=rules, issueStatuses="OPEN,CONFIRMED", ps=500, p=page)
    for i in d["issues"]:
        out.append({"rule": i["rule"], "file": i["component"].split(":", 1)[1], "line": i.get("line"), "message": i["message"]})
    if page * 500 >= d["paging"]["total"]:
        break
    page += 1
json.dump(out, sys.stdout, indent=1)
