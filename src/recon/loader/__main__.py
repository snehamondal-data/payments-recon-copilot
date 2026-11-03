"""python -m recon.loader [--from-beginning] [--follow]"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone

from dotenv import load_dotenv

from recon.config import load_settings
from recon.loader import run_loader
from recon.warehouse import DuckDBClient

DEFAULT_GROUP = "recon-loader"


def main(argv: list[str] | None = None) -> None:
    load_dotenv(".env")
    settings = load_settings()
    parser = argparse.ArgumentParser(description="Load the source topics into the warehouse "
                                                 "(DuckDB, or Snowflake with WAREHOUSE_ENGINE=snowflake).")
    parser.add_argument("--group", default=DEFAULT_GROUP)
    parser.add_argument("--from-beginning", action="store_true",
                        help="replay: use a fresh consumer group that starts at the earliest offset")
    parser.add_argument("--follow", action="store_true", help="keep consuming instead of stopping at the end")
    args = parser.parse_args(argv)

    group = args.group
    if args.from_beginning:
        group = f"{args.group}-replay-{datetime.now(timezone.utc):%Y%m%dT%H%M%S}"
    if settings.warehouse_engine == "snowflake":
        from recon.warehouse.snowflake_client import SnowflakeClient

        wh = SnowflakeClient(settings.snowflake, settings.snowflake.loader)
    else:
        wh = DuckDBClient(settings.duckdb_path)
    try:
        stats = run_loader(wh, bootstrap=settings.kafka_bootstrap, group_id=group,
                           topic_prefix=settings.topic_prefix, fx_rates_path=settings.fx_rates_path,
                           stop_at_eof=not args.follow)
    finally:
        wh.close()
    print(f"engine={settings.warehouse_engine} group={group} received={stats.received} valid={stats.valid} rows_inserted={stats.rows_inserted} "
          f"rejected={stats.rejected} dead_lettered={stats.dead_lettered}")
    print(f"inserted by table: {stats.inserted}")


if __name__ == "__main__":
    main()
