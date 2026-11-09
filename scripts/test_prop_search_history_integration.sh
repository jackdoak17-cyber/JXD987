#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 || ! -f "$1/tests/sql/prop-search-playing-range-regression.sql" ]]; then
  echo "Usage: $0 /absolute/path/to/statswebsite-web-prototype" >&2
  exit 2
fi
website_repo="$(cd "$1" && pwd)"
worker_repo="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
for inherited_setting in PGHOSTADDR PGHOST PGPORT PGSERVICE PGSERVICEFILE \
  PGDATABASE PGUSER PGPASSWORD PGPASSFILE PGOPTIONS SUPABASE_DB_URL \
  SUPABASE_DB_URL_SESSION; do
  if [[ ${!inherited_setting+x} ]]; then
    echo "Refusing inherited database connection settings." >&2
    exit 2
  fi
done

tmp_dir="$(mktemp -d /tmp/oddssearch-prop-worker.XXXXXX)"
db_dir="$tmp_dir/data"
socket_dir="$tmp_dir/socket"
db_port=55439
db_user="$(id -un)"
mkdir -p "$socket_dir"

run_local_pg() {
  env -i PATH="$PATH" PGHOST="$socket_dir" PGPORT="$db_port" \
    PGUSER="$db_user" "$@"
}

assert_local_database() {
  local name="$1" actual
  actual="$(run_local_pg psql -X -qAt -d "$name" -c \
    "select current_database(), case when inet_server_addr() is null then 'socket' else 'tcp' end, current_setting('data_directory')")"
  if [[ "$actual" != "$name|socket|$db_dir" ]]; then
    echo "Refusing integration DDL outside the disposable database." >&2
    exit 2
  fi
}

cleanup() {
  if [[ -d "$db_dir" ]]; then
    pg_ctl -D "$db_dir" -m fast stop >/dev/null 2>&1 || true
  fi
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

initdb -D "$db_dir" -A trust -U "$db_user" >/dev/null
pg_ctl -D "$db_dir" -o "-k $socket_dir -h '' -p $db_port" -w start >/dev/null
assert_local_database postgres
run_local_pg createdb sparse_range_test
assert_local_database sparse_range_test
run_local_pg psql -X -q -d sparse_range_test -v ON_ERROR_STOP=1 -c \
  'create role anon; create role authenticated; create role service_role bypassrls;'

# The website replay is normally rollback-only. Commit is permitted only
# after the socket and data-directory identity checks above.
if ! (cd "$website_repo" && run_local_pg psql -X -q -d sparse_range_test \
  -v ON_ERROR_STOP=1 -v commit_test=1 \
  -f tests/sql/prop-search-playing-range-regression.sql \
  >"$tmp_dir/website-replay.log" 2>&1); then
  cat "$tmp_dir/website-replay.log" >&2
  exit 1
fi

run_local_pg psql -X -q -d sparse_range_test -v ON_ERROR_STOP=1 -c \
  "update public.fixture_player_statistics set value=0 where fixture_id=1004 and player_id=1 and type_id=86" >/dev/null
expected_md5="$(run_local_pg psql -X -qAt -d sparse_range_test -v ON_ERROR_STOP=1 -c \
  "select md5(pg_get_functiondef(to_regprocedure('prop_search_history_v1.refresh_fixture(bigint)')))" )"
local_dsn="dbname=sparse_range_test host=$socket_dir port=$db_port user=$db_user"

dry_report="$(env -i PATH="$PATH" SUPABASE_DB_URL="$local_dsn" \
  python3 "$worker_repo/scripts/refresh_prop_search_history.py")"
python3 -c 'import json,sys; r=json.load(sys.stdin); assert r["status"]=="dry_run" and r["dirty_before"]==1 and r["unrefreshed_before"]==0' \
  <<<"$dry_report"

apply_report="$(env -i PATH="$PATH" SUPABASE_DB_URL="$local_dsn" \
  python3 "$worker_repo/scripts/refresh_prop_search_history.py" \
  --apply --max-fixtures 1 --max-seconds 20 --max-wal-bytes 1000000 \
  --expected-refresh-md5 "$expected_md5")"
python3 -c 'import json,sys; r=json.load(sys.stdin); assert r["status"]=="fixture_ceiling" and r["processed_fixtures"]==1 and r["source_kinds"]=={"dirty":1} and 0<r["accounted_wal_bytes"]<=1000000' \
  <<<"$apply_report"

run_local_pg psql -X -q -d sparse_range_test -v ON_ERROR_STOP=1 <<'SQL'
DO $verify_worker_sql_pair$
DECLARE
  target_date date := ((date_trunc('day', now()) + interval '1 day 15 hours') AT TIME ZONE 'Europe/London')::date;
  mismatch_count integer;
BEGIN
  IF EXISTS (SELECT 1 FROM prop_search_history_v1.dirty_fixtures)
     OR NOT EXISTS (
       SELECT 1 FROM prop_search_history_v1.appearances
       WHERE fixture_id=1004 AND player_id=1 AND metrics->>'86'='0'
     ) THEN
    RAISE EXCEPTION 'worker did not refresh the exact projected source row';
  END IF;
  WITH guarded AS (
    SELECT * FROM public.player_streak_playing_maintained_counts_v1(
      ARRAY[target_date], ARRAY[8], 86, 5, 1, 3
    )
  ), installed AS (
    SELECT target_date AS playing_date, 8 AS league_id, counts.*
    FROM public.player_streak_playing_on_date_counts(target_date, 8, 86, 5, 1, 3) counts
  )
  SELECT count(*) INTO mismatch_count
  FROM ((SELECT * FROM guarded EXCEPT ALL SELECT * FROM installed)
        UNION ALL (SELECT * FROM installed EXCEPT ALL SELECT * FROM guarded)) differences;
  IF mismatch_count <> 0 THEN
    RAISE EXCEPTION 'worker-refreshed result disagrees with installed source';
  END IF;
END
$verify_worker_sql_pair$;
SQL

echo "Exact website SQL plus bounded JXD worker integration: PASS"
