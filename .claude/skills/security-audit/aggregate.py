#!/usr/bin/env python3
"""
aggregate.py — Aggregate security audit results into a structured report.

Usage: python3 aggregate.py <output_dir> [--year YEAR] [--format json|markdown]

Reads per-commit JSON files produced by analyze_commit.sh and produces:
  1. CWE/vulnerability type classification (inferred from commit messages + diff patterns)
  2. Blast radius distribution (callers, files affected)
  3. Time-to-introduction analysis (git blame → original commit date)
  4. Root cause theme clustering
  5. Per-year trends
"""

import json
import glob
import os
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime

# ── Vulnerability classification patterns ────────────────────────
# Maps regex patterns (on commit message + diff) to CWE categories
VULN_PATTERNS = [
    # Access control / authorization
    (r'(?i)(authori[sz]ation|access.control|permission|unauthorized|IDOR|role.assign|non.member|policy)',
     'CWE-284', 'Improper Access Control'),
    # Authentication bypass
    (r'(?i)(2FA|two.factor|bypass|authentication|login|session|passkey|password)',
     'CWE-287', 'Improper Authentication'),
    # XSS
    (r'(?i)(XSS|cross.site.script|sanitiz|escap|html.inject|wrapNode)',
     'CWE-79', 'Cross-site Scripting'),
    # SSRF
    (r'(?i)(SSRF|server.side.request|DNS.rebind)',
     'CWE-918', 'Server-Side Request Forgery'),
    # Path traversal
    (r'(?i)(path.traversal|path.inject|directory.traversal|\.\.\/)',
     'CWE-22', 'Path Traversal'),
    # Injection (command, SQL, etc)
    (r'(?i)(inject|command.exec|popen|shell|sql.inject)',
     'CWE-78', 'OS Command Injection'),
    # Information disclosure
    (r'(?i)(disclos|leak|expos|hide.*private|redact|confidential|token.leak)',
     'CWE-200', 'Information Disclosure'),
    # DoS / resource exhaustion
    (r'(?i)(DoS|denial.of.service|ReDoS|timeout|limit|exhaust|large|unbounded|complex)',
     'CWE-400', 'Resource Exhaustion / DoS'),
    # Input validation
    (r'(?i)(valid|sanitiz|overflow|size.limit|parse|JSON.*depth)',
     'CWE-20', 'Improper Input Validation'),
    # CSRF
    (r'(?i)(CSRF|cross.site.request|forgery)',
     'CWE-352', 'Cross-Site Request Forgery'),
    # Cache poisoning
    (r'(?i)(cache.poison|cache.key)',
     'CWE-349', 'Cache Poisoning'),
]

# ── Root cause patterns (from code structure) ────────────────────
ROOT_CAUSE_PATTERNS = [
    (r'(?i)(concern|module|mixin|include)', 'shared-concern-bypass',
     'Shared concern/mixin with inconsistent enforcement'),
    (r'(?i)(controller|action|before_action)', 'controller-auth-gap',
     'Missing or inconsistent controller-level authorization'),
    (r'(?i)(api|endpoint|graphql|mutation)', 'api-surface-gap',
     'API endpoint missing authorization or validation'),
    (r'(?i)(serializ|render|view|template|html)', 'output-encoding',
     'Missing output encoding or template escaping'),
    (r'(?i)(param|input|request|user_input)', 'input-trust',
     'Untrusted input accepted without validation'),
    (r'(?i)(service|worker|job|sidekiq)', 'service-layer-gap',
     'Service layer missing authorization check'),
    (r'(?i)(scope|policy|ability|can\?)', 'policy-gap',
     'Policy/scope not applied or applied inconsistently'),
    (r'(?i)(config|setting|flag|feature)', 'config-drift',
     'Security-relevant configuration drift or default'),
]


def classify_vuln(commit_data):
    """Classify a commit into CWE category based on message and file paths."""
    text = commit_data.get('message', '') + ' ' + ' '.join(
        commit_data.get('files', {}).get('changed', []))

    for pattern, cwe, name in VULN_PATTERNS:
        if re.search(pattern, text):
            return cwe, name
    return 'CWE-Other', 'Unclassified'


def classify_root_cause(commit_data):
    """Infer root cause theme from changed file paths and definitions."""
    text = ' '.join(commit_data.get('files', {}).get('changed', []))
    defs = ' '.join(d.get('fqn', '') for d in commit_data.get('definitions', []))
    combined = text + ' ' + defs

    causes = []
    for pattern, key, desc in ROOT_CAUSE_PATTERNS:
        if re.search(pattern, combined):
            causes.append((key, desc))
    return causes or [('unknown', 'Root cause not determined from code structure')]


def compute_introduction_lag(commit_data):
    """Compute time between vulnerability introduction and fix."""
    fix_date_str = commit_data.get('date', '')
    if not fix_date_str:
        return None

    try:
        fix_date = datetime.fromisoformat(fix_date_str.replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return None

    blame = commit_data.get('blame', [])
    if not blame:
        return None

    intro_dates = []
    for b in blame:
        try:
            d = datetime.fromisoformat(b['intro_date'].replace('Z', '+00:00'))
            intro_dates.append(d)
        except (ValueError, TypeError, KeyError):
            continue

    if not intro_dates:
        return None

    earliest = min(intro_dates)
    return (fix_date - earliest).days


def aggregate(output_dir, year_filter=None):
    """Main aggregation function."""
    files = sorted(glob.glob(os.path.join(output_dir, '*.json')))
    commits = []
    for f in files:
        if os.path.basename(f) in ('manifest.json', 'aggregate.json', 'report.json'):
            continue
        try:
            data = json.load(open(f))
            if 'error' in data and 'commit' not in data.get('files', {}):
                continue
            commits.append(data)
        except Exception:
            continue

    if year_filter:
        commits = [c for c in commits if c.get('date', '').startswith(str(year_filter))]

    # ── Classification ───────────────────────────────────────────
    cwe_counts = Counter()
    cwe_examples = defaultdict(list)
    root_cause_counts = Counter()
    blast_radii = []
    intro_lags = []
    by_year = defaultdict(lambda: {'count': 0, 'cwe': Counter(), 'blast': []})

    for c in commits:
        cwe, cwe_name = classify_vuln(c)
        cwe_counts[(cwe, cwe_name)] += 1
        cwe_examples[(cwe, cwe_name)].append({
            'commit': c['commit'][:12],
            'message': c.get('message', '')[:100],
            'date': c.get('date', '')[:10],
            'blast_radius': c.get('blast_radius', {}).get('caller_edges', 0),
        })

        for key, desc in classify_root_cause(c):
            root_cause_counts[(key, desc)] += 1

        br = c.get('blast_radius', {})
        blast_radii.append({
            'commit': c['commit'][:12],
            'callers': br.get('caller_edges', 0),
            'files': br.get('caller_files', 0),
            'message': c.get('message', '')[:80],
        })

        lag = compute_introduction_lag(c)
        if lag is not None and lag >= 0:
            intro_lags.append({
                'commit': c['commit'][:12],
                'days': lag,
                'message': c.get('message', '')[:80],
            })

        year = c.get('date', '')[:4]
        if year:
            by_year[year]['count'] += 1
            by_year[year]['cwe'][(cwe, cwe_name)] += 1
            by_year[year]['blast'].append(br.get('caller_edges', 0))

    # ── Build report ─────────────────────────────────────────────
    report = {
        'summary': {
            'total_commits_analyzed': len(commits),
            'year_filter': year_filter,
            'date_range': {
                'earliest': min((c.get('date', '') for c in commits), default=''),
                'latest': max((c.get('date', '') for c in commits), default=''),
            },
        },
        'vulnerability_types': [
            {
                'cwe': cwe,
                'name': name,
                'count': count,
                'examples': cwe_examples[(cwe, name)][:5],
            }
            for (cwe, name), count in cwe_counts.most_common()
        ],
        'root_causes': [
            {'theme': key, 'description': desc, 'count': count}
            for (key, desc), count in root_cause_counts.most_common()
        ],
        'blast_radius': {
            'distribution': {
                'p50': sorted(b['callers'] for b in blast_radii)[len(blast_radii)//2] if blast_radii else 0,
                'p90': sorted(b['callers'] for b in blast_radii)[int(len(blast_radii)*0.9)] if blast_radii else 0,
                'p99': sorted(b['callers'] for b in blast_radii)[int(len(blast_radii)*0.99)] if blast_radii else 0,
                'max': max((b['callers'] for b in blast_radii), default=0),
            },
            'top_20': sorted(blast_radii, key=lambda b: b['callers'], reverse=True)[:20],
        },
        'introduction_lag': {
            'distribution': {
                'p50': sorted(l['days'] for l in intro_lags)[len(intro_lags)//2] if intro_lags else 0,
                'p90': sorted(l['days'] for l in intro_lags)[int(len(intro_lags)*0.9)] if intro_lags else 0,
                'max': max((l['days'] for l in intro_lags), default=0),
                'avg': sum(l['days'] for l in intro_lags) / len(intro_lags) if intro_lags else 0,
            },
            'longest_20': sorted(intro_lags, key=lambda l: l['days'], reverse=True)[:20],
        },
        'by_year': {
            year: {
                'count': data['count'],
                'top_cwe': [
                    {'cwe': cwe, 'name': name, 'count': cnt}
                    for (cwe, name), cnt in data['cwe'].most_common(5)
                ],
                'blast_radius_avg': sum(data['blast']) / len(data['blast']) if data['blast'] else 0,
            }
            for year, data in sorted(by_year.items())
        },
    }

    return report


def to_markdown(report):
    """Convert aggregate report to markdown."""
    lines = []
    s = report['summary']
    lines.append(f"# Security Posture Audit")
    lines.append(f"")
    lines.append(f"**Commits analyzed:** {s['total_commits_analyzed']}")
    lines.append(f"**Date range:** {s['date_range']['earliest'][:10]} to {s['date_range']['latest'][:10]}")
    if s['year_filter']:
        lines.append(f"**Year filter:** {s['year_filter']}")
    lines.append("")

    lines.append("## Vulnerability Types")
    lines.append("")
    lines.append("| CWE | Category | Count |")
    lines.append("|-----|----------|-------|")
    for v in report['vulnerability_types']:
        lines.append(f"| {v['cwe']} | {v['name']} | {v['count']} |")
    lines.append("")

    lines.append("## Root Cause Themes")
    lines.append("")
    lines.append("| Theme | Description | Count |")
    lines.append("|-------|-------------|-------|")
    for r in report['root_causes']:
        lines.append(f"| {r['theme']} | {r['description']} | {r['count']} |")
    lines.append("")

    lines.append("## Blast Radius Distribution")
    lines.append("")
    br = report['blast_radius']['distribution']
    lines.append(f"- **p50:** {br['p50']} caller edges")
    lines.append(f"- **p90:** {br['p90']} caller edges")
    lines.append(f"- **p99:** {br['p99']} caller edges")
    lines.append(f"- **max:** {br['max']} caller edges")
    lines.append("")
    lines.append("### Top 20 by Blast Radius")
    lines.append("")
    lines.append("| Commit | Callers | Files | Description |")
    lines.append("|--------|---------|-------|-------------|")
    for b in report['blast_radius']['top_20']:
        lines.append(f"| `{b['commit']}` | {b['callers']} | {b['files']} | {b['message']} |")
    lines.append("")

    lines.append("## Vulnerability Introduction Lag")
    lines.append("")
    il = report['introduction_lag']['distribution']
    lines.append(f"- **Median:** {il['p50']} days")
    lines.append(f"- **p90:** {il['p90']} days")
    lines.append(f"- **Max:** {il['max']} days")
    lines.append(f"- **Average:** {il['avg']:.0f} days")
    lines.append("")

    lines.append("## Trends by Year")
    lines.append("")
    lines.append("| Year | Fixes | Avg Blast Radius | Top CWE |")
    lines.append("|------|-------|------------------|---------|")
    for year, data in sorted(report['by_year'].items()):
        top_cwe = data['top_cwe'][0]['name'] if data['top_cwe'] else 'N/A'
        lines.append(f"| {year} | {data['count']} | {data['blast_radius_avg']:.1f} | {top_cwe} |")
    lines.append("")

    return '\n'.join(lines)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('output_dir')
    parser.add_argument('--year', type=int, default=None)
    parser.add_argument('--format', choices=['json', 'markdown'], default='json')
    args = parser.parse_args()

    report = aggregate(args.output_dir, args.year)

    if args.format == 'markdown':
        print(to_markdown(report))
    else:
        print(json.dumps(report, indent=2))
