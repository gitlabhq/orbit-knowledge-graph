#!/usr/bin/env bash
# Run export.rb inside the CNG toolbox image against an empty database with the
# image's own schema. Writes the snapshot to the path in $1.
set -euo pipefail
OUT="$(realpath -m "${1:?usage: export-in-toolbox.sh <output.json>}")"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DB_HOST="${RAILS_CONTRACT_DB_HOST:-postgres}"

cd /srv/gitlab
sed -i "s/host: localhost/host: ${DB_HOST}/" config/database.yml
PGPASSWORD="secure password" psql -q -h "$DB_HOST" -U git -d gitlabhq_production \
  -v ON_ERROR_STOP=1 -f db/structure.sql > /dev/null
bin/rails runner - < "$SCRIPT_DIR/export.rb" | sed -n '/^{$/,/^}$/p' > "$OUT"
test -s "$OUT"
