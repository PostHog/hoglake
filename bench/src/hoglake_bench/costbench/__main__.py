"""Independent CLI; the top-level hoglake-bench command delegates here."""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from .client import TrinoClient
from .queries import load_queries
from .runner import BenchmarkAborted, read_records, run_queries, summarize


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="hoglake-bench costbench")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run", help="run each query three times consecutively")
    run.add_argument("--trino-url", required=True)
    run.add_argument("--user", default="benchmark")
    run.add_argument(
        "--password-env",
        help="environment variable containing the HTTPS Basic Auth password",
    )
    run.add_argument(
        "--ca-file", type=Path, help="CA certificate bundle for HTTPS verification"
    )
    run.add_argument("--catalog", default="hoglake")
    run.add_argument("--schema", default="costbench")
    run.add_argument("--table", default="hits")
    run.add_argument("--query-timeout", type=float, default=1800)
    run.add_argument(
        "--hourly-usd",
        type=float,
        required=True,
        help="entire fixed stack's hourly compute rate",
    )
    run.add_argument(
        "--output",
        type=Path,
        required=True,
        help="new raw JSONL file; existing files are refused",
    )
    run.add_argument(
        "--metadata", type=Path, help="JSON with public version/hardware/layout fields"
    )
    run.add_argument(
        "--queries", help="comma-separated query numbers for a partial smoke run"
    )
    run.add_argument(
        "--query-cache",
        type=Path,
        default=Path(os.environ.get("XDG_CACHE_HOME", str(Path.home() / ".cache")))
        / "hoglake-bench/costbench-2025/queries.sql",
    )
    report = commands.add_parser(
        "report",
        help="report full coverage, best-of-three times and query compute cost",
    )
    report.add_argument("--input", type=Path, required=True)
    report.add_argument("--output", type=Path)
    report.add_argument("--timing", choices=("client", "server"), default="client")
    report.add_argument(
        "--hourly-usd", type=float, help="override the recorded compute rate"
    )
    compare = commands.add_parser(
        "compare", help="compare full 1B-row reports with pinned 2025 warehouse records"
    )
    compare.add_argument(
        "--input",
        nargs="+",
        type=Path,
        required=True,
        help="one or more JSON reports from the report command",
    )
    compare.add_argument("--output", type=Path, required=True)
    compare.add_argument("--markdown", type=Path)
    compare.add_argument(
        "--reference-cache",
        type=Path,
        default=Path(os.environ.get("XDG_CACHE_HOME", str(Path.home() / ".cache")))
        / "hoglake-bench/costbench-2025/references",
    )
    # Load is deliberately independent of the existing synthetic seed command.
    from .load import add_load_arguments

    add_load_arguments(
        commands.add_parser(
            "load", help="load the public seed and expand to the requested row count"
        )
    )
    from .cluster import add_cluster_arguments

    add_cluster_arguments(
        commands.add_parser(
            "cluster", help="operate the portable nine-worker Kubernetes fixture"
        )
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command == "cluster":
        from .cluster import cluster_main

        return cluster_main(args)
    if args.command == "load":
        from .load import load_main

        return load_main(args)
    if args.command == "compare":
        from .comparison import compare_files, markdown_table

        comparison = compare_files(args.input, args.reference_cache)
        args.output.write_text(json.dumps(comparison, indent=2, allow_nan=False) + "\n")
        if args.markdown:
            args.markdown.write_text(markdown_table(comparison))
        return 0 if comparison["complete"] else 2
    if args.command == "run":
        password = os.environ.get(args.password_env) if args.password_env else None
        if args.password_env and not password:
            parser.error(
                f"environment variable {args.password_env} is missing or empty"
            )
        selected = (
            [int(value) for value in args.queries.split(",")] if args.queries else None
        )
        queries = load_queries(args.query_cache, args.catalog, args.schema, args.table)
        metadata = json.loads(args.metadata.read_text()) if args.metadata else {}
        with TrinoClient(
            args.trino_url,
            user=args.user,
            catalog=args.catalog,
            schema=args.schema,
            timeout_s=args.query_timeout,
            password=password,
            ca_file=args.ca_file,
        ) as client:
            try:
                run_queries(
                    client,
                    queries,
                    args.output,
                    selected_queries=selected,
                    hourly_usd=args.hourly_usd,
                    metadata=metadata,
                )
            except BenchmarkAborted as exc:
                print(str(exc), file=sys.stderr)
                return 3
        report = summarize(read_records(args.output))
        selected = selected if selected is not None else list(range(1, 44))
        return (
            2
            if report["failed_attempts"]
            or any(query in report["missing_queries"] for query in selected)
            else 0
        )
    result = summarize(
        read_records(args.input), timing=args.timing, hourly_usd=args.hourly_usd
    )
    text = json.dumps(result, indent=2, allow_nan=False) + "\n"
    if args.output:
        args.output.write_text(text)
    else:
        print(text, end="")
    return 0 if result["complete"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
