import json
import sys

RULES = ("java:S1144", "java:S1068")


def keyed(findings, files, rule):
    return {(f["file"], f["line"]) for f in findings if f["rule"] == rule and files.get(f["file"]) == "FIL"}


work = sys.argv[1]
totals = {rule: [0, 0, 0] for rule in RULES}
for line in open(sys.argv[2]):
    project, repo, _ = line.split()
    files = json.load(open(f"{work}/{repo}.files.json"))
    sonar = json.load(open(f"{work}/{repo}.sonar.json"))
    ours = json.load(open(f"{work}/{repo}.orbit.json"))
    row = []
    for rule in RULES:
        s, o = keyed(sonar, files, rule), keyed(ours, files, rule)
        totals[rule][0] += len(s)
        totals[rule][1] += len(o)
        totals[rule][2] += len(s & o)
        row.append(f"{rule} {len(s & o)}/{len(s)} +{len(o - s)}")
        for path, ln in sorted(s - o)[:5]:
            row.append(f"\n    missed {path}:{ln}")
        for path, ln in sorted(o - s)[:5]:
            row.append(f"\n    extra  {path}:{ln}")
    print(f"{repo:22s} " + "  ".join(row))
for rule, (s, o, both) in totals.items():
    print(f"TOTAL {rule}: sonar {s} orbit {o} both {both} recall {both / max(s, 1):.1%} precision {both / max(o, 1):.1%}")
