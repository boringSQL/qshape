#!/usr/bin/env bash
# Captures the generic plan of every case in testdata/attribute/cases.tsv on
# each supported PG major, the same way `qshape attribute` asks for it
# (PREPARE + EXPLAIN EXECUTE under force_generic_plan). Tables are empty, so
# a rerun with the same tags produces identical files.
#
#   scripts/capture-attr-plans.sh    # needs docker
set -euo pipefail
cd "$(dirname "$0")/.."

TAGS=(13.23 14.24 15.19 16.15 17.11 18.6)
DIR=testdata/attribute
container=""
trap '[ -n "$container" ] && docker rm -f "$container" >/dev/null 2>&1 || true' EXIT

for tag in "${TAGS[@]}"; do
	major=${tag%%.*}
	container=qshape-attr-pg$major
	docker rm -f "$container" >/dev/null 2>&1 || true
	docker run -d --rm --name "$container" -e POSTGRES_HOST_AUTH_METHOD=trust \
		"postgres:$tag-alpine" >/dev/null
	# The init-time server listens on the socket only; TCP means the real one.
	until docker exec "$container" pg_isready -q -h 127.0.0.1 -U postgres; do sleep 0.5; done

	psql() { docker exec -i "$container" psql -X -q -At -v ON_ERROR_STOP=1 -h 127.0.0.1 -U postgres; }
	psql <"$DIR/schema.sql"

	mkdir -p "$DIR/pg$major"
	while IFS=$'\t' read -r name sql; do
		nparams=$(grep -o '\$[0-9]\+' <<<"$sql" | sort -u | wc -l | tr -d ' ')
		nulls=$(printf 'NULL,%.0s' $(seq "$nparams"))
		psql >"$DIR/pg$major/$name.json" <<-EOF
			BEGIN;
			SET LOCAL plan_cache_mode = force_generic_plan;
			PREPARE p AS $sql;
			EXPLAIN (FORMAT JSON) EXECUTE p(${nulls%,});
			COMMIT;
		EOF
	done <"$DIR/cases.tsv"

	docker rm -f "$container" >/dev/null
	container=""
	echo "pg$major ($tag): $(wc -l <"$DIR/cases.tsv" | tr -d ' ') plans"
done
