#!/usr/bin/env bash
# Run export.rb in the CNG toolbox image against an empty database with the image's own schema; write the snapshot to $1.
set -euo pipefail
out="${1:?usage: export-in-toolbox.sh <output.json>}"
sed -i "s/host: localhost/host: postgres/" /srv/gitlab/config/database.yml
PGPASSWORD="secure password" psql -q -h postgres -U git -d gitlabhq_production -v ON_ERROR_STOP=1 -f /srv/gitlab/db/structure.sql > /dev/null
(cd /srv/gitlab && bin/rails runner -) < "$(dirname "$0")/export.rb" | sed -n '/^{$/,/^}$/p' > "$out"
test -s "$out"
