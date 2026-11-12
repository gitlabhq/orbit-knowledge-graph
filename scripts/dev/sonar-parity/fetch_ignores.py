import json
import sys
import urllib.parse
import urllib.request

S = "https://sonarcloud.io/api"
query = urllib.parse.urlencode({"component": sys.argv[1], "keys": "sonar.issue.ignore.multicriteria"})
with urllib.request.urlopen(f"{S}/settings/values?{query}", timeout=60) as response:
    settings = json.load(response).get("settings", [])
json.dump(settings[0]["fieldValues"] if settings else [], sys.stdout)
