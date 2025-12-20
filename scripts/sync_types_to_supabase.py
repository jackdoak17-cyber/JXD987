#!/usr/bin/env python3
import argparse

from jxd.sportmonks_client import SportMonksClient
from scripts.export_to_supabase import require_env, upsert_table


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Compute payload size without sending to Supabase")
    args = parser.parse_args()

    require_env(args.dry_run)

    client = SportMonksClient(base_url="https://api.sportmonks.com/v3/core/")
    rows = []
    for row in client.fetch_collection("types", per_page=200):
        rows.append(
            {
                "id": row.get("id"),
                "name": row.get("name"),
                "code": row.get("code"),
                "developer_name": row.get("developer_name"),
                "model_type": row.get("model_type"),
                "stat_group": row.get("stat_group"),
            }
        )

    exported = upsert_table("types", rows, "id", args.dry_run)
    print(f"types_exported={exported} total={len(rows)}")


if __name__ == "__main__":
    main()
