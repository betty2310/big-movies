"""
CLI for PostgreSQL loader.

Usage:
    uv run load-postgres                    # Load all tables
    uv run load-postgres --tables dim_movie,fact_movie_metrics
    uv run load-postgres --validate         # Validate only
    uv run load-postgres --no-truncate      # Append mode
"""

import argparse
import sys

from dotenv import load_dotenv


def main():
    load_dotenv()

    parser = argparse.ArgumentParser(
        description="Load gold layer data from S3 to PostgreSQL"
    )
    parser.add_argument(
        "--tables",
        type=str,
        default=None,
        help="Comma-separated list of tables to load (default: all)",
    )
    parser.add_argument(
        "--no-truncate",
        action="store_true",
        help="Don't truncate tables before insert (append mode)",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Only validate existing data, don't load",
    )

    args = parser.parse_args()

    from loader.loader import load_all_tables, validate_load

    tables = None
    if args.tables:
        tables = [t.strip() for t in args.tables.split(",")]

    if args.validate:
        results = validate_load(tables)
        orphan_facts = results.get("_orphan_facts", 0)
        failed = isinstance(orphan_facts, int) and orphan_facts > 0
        sys.exit(1 if failed else 0)

    results = load_all_tables(
        tables=tables,
        truncate=not args.no_truncate,
    )

    failed = any(count < 0 for count in results.values())
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
