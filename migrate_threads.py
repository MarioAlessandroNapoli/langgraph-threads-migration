#!/usr/bin/env python3
"""
LangGraph Threads Export Tool

Export threads, checkpoints, and conversation history from LangGraph Cloud.
Save to JSON files, PostgreSQL databases, or migrate to another deployment.

Usage:
    python migrate_threads.py --source-url <URL> --export-json threads.json
    python migrate_threads.py --source-url <URL> --export-postgres
    python migrate_threads.py --source-url <SRC> --target-url <TGT> --full
"""

import argparse
import asyncio
import os
import sys

from dotenv import load_dotenv
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table

from langgraph_export import ThreadMigrator
from langgraph_export.exporters import JSONExporter

# Load environment variables
load_dotenv()

console = Console()


async def run_export_json(
    source_url: str,
    api_key: str,
    output_file: str,
    test_single: bool = False,
) -> None:
    """Export threads to JSON file."""
    console.print(Panel.fit(
        "[bold cyan]Exporting threads to JSON[/bold cyan]",
        border_style="cyan",
    ))

    async with ThreadMigrator(source_url=source_url, api_key=api_key) as migrator:
        json_exporter = migrator.add_json_exporter(output_file)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("[cyan]Exporting...", total=None)

            def update_progress(count: int, message: str) -> None:
                progress.update(task, completed=count, description=f"[cyan]{message}")

            limit = 1 if test_single else None
            stats = await migrator.export_threads(limit=limit, progress_callback=update_progress)

        # Display results
        console.print(f"\n[green]✓[/green] Threads exported: {stats.threads_exported}")
        console.print(f"[green]✓[/green] Checkpoints exported: {stats.checkpoints_exported}")
        console.print(f"[green]✓[/green] Output file: [bold]{output_file}[/bold]")
        console.print(f"[green]✓[/green] File size: {json_exporter.get_file_size_mb():.2f} MB")


async def run_export_postgres(
    source_url: str,
    api_key: str,
    database_url: str,
    output_file: str,
    test_single: bool = False,
) -> None:
    """Export threads to PostgreSQL (and JSON backup)."""
    console.print(Panel.fit(
        "[bold cyan]Exporting threads to PostgreSQL[/bold cyan]",
        border_style="cyan",
    ))

    async with ThreadMigrator(source_url=source_url, api_key=api_key) as migrator:
        json_exporter = migrator.add_json_exporter(output_file)
        pg_exporter = migrator.add_postgres_exporter(database_url)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("[cyan]Exporting...", total=None)

            def update_progress(count: int, message: str) -> None:
                progress.update(task, completed=count, description=f"[cyan]{message}")

            limit = 1 if test_single else None
            stats = await migrator.export_threads(limit=limit, progress_callback=update_progress)

        # Get PostgreSQL stats
        db_stats = await pg_exporter.get_database_stats()

        # Display results
        console.print(f"\n[green]✓[/green] Threads exported: {stats.threads_exported}")
        console.print(f"[green]✓[/green] Checkpoints exported: {stats.checkpoints_exported}")
        console.print(f"[green]✓[/green] JSON backup: [bold]{output_file}[/bold] ({json_exporter.get_file_size_mb():.2f} MB)")
        console.print(f"[green]✓[/green] PostgreSQL: {db_stats['threads']} threads, {db_stats['checkpoints']} checkpoints")


async def run_import_json(
    target_url: str,
    api_key: str,
    input_file: str,
    dry_run: bool = False,
) -> None:
    """Import threads from JSON file."""
    console.print(Panel.fit(
        "[bold green]Importing threads from JSON[/bold green]",
        border_style="green",
    ))

    # Load threads from JSON
    threads = JSONExporter.load_threads(input_file)
    info = JSONExporter.get_export_info(input_file)

    console.print(f"[cyan]Source:[/cyan] {info['source']}")
    console.print(f"[cyan]Export date:[/cyan] {info['export_date']}")
    console.print(f"[cyan]Threads to import:[/cyan] {len(threads)}")

    if dry_run:
        console.print("\n[yellow]⚠ DRY-RUN MODE: No changes will be made[/yellow]")

    async with ThreadMigrator(target_url=target_url, api_key=api_key) as migrator:
        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("[green]Importing...", total=len(threads))

            def update_progress(count: int, message: str) -> None:
                progress.update(task, completed=count, description=f"[green]{message}")

            results = await migrator.import_threads(
                threads,
                dry_run=dry_run,
                progress_callback=update_progress,
            )

        # Display results
        table = Table(title="Import Summary")
        table.add_column("Status", style="cyan")
        table.add_column("Count", justify="right", style="magenta")

        table.add_row("Created", str(results["created"]))
        table.add_row("Skipped (exists)", str(results["skipped"]))
        table.add_row("Failed", str(results["failed"]))
        table.add_row("Checkpoints", str(results.get("checkpoints", 0)))
        table.add_row("Total threads", str(len(threads)))

        console.print(table)


async def run_full_migration(
    source_url: str,
    target_url: str,
    api_key: str,
    backup_file: str,
    dry_run: bool = False,
    test_single: bool = False,
) -> None:
    """Full migration: export + import + validate."""
    # Phase 1: Export
    console.print(Panel.fit(
        "[bold cyan]Phase 1: Export threads from source[/bold cyan]",
        border_style="cyan",
    ))

    async with ThreadMigrator(
        source_url=source_url,
        target_url=target_url,
        api_key=api_key,
    ) as migrator:
        json_exporter = migrator.add_json_exporter(backup_file)

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("[cyan]Exporting...", total=None)

            def update_progress(count: int, message: str) -> None:
                progress.update(task, completed=count, description=f"[cyan]{message}")

            limit = 1 if test_single else None
            threads = await migrator.fetch_all_threads(limit=limit, progress_callback=update_progress)

        # Export to JSON
        for thread in threads:
            await json_exporter.export_thread(thread)
        await json_exporter.finalize()

        console.print(f"\n[green]✓[/green] Exported {len(threads)} threads")
        console.print(f"[green]✓[/green] Backup: {backup_file} ({json_exporter.get_file_size_mb():.2f} MB)")

        # Phase 2: Import
        console.print(Panel.fit(
            f"[bold green]Phase 2: Import {len(threads)} threads to target[/bold green]",
            border_style="green",
        ))

        if dry_run:
            console.print("[yellow]⚠ DRY-RUN MODE: No changes will be made[/yellow]")

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("[green]Importing...", total=len(threads))

            def update_progress2(count: int, message: str) -> None:
                progress.update(task, completed=count, description=f"[green]{message}")

            results = await migrator.import_threads(
                threads,
                dry_run=dry_run,
                progress_callback=update_progress2,
            )

        # Display import results
        table = Table(title="Import Summary")
        table.add_column("Status", style="cyan")
        table.add_column("Count", justify="right", style="magenta")

        table.add_row("Created", str(results["created"]))
        table.add_row("Skipped (exists)", str(results["skipped"]))
        table.add_row("Failed", str(results["failed"]))
        table.add_row("Checkpoints", str(results.get("checkpoints", 0)))

        console.print(table)

        # Phase 3: Validate
        console.print(Panel.fit(
            "[bold blue]Phase 3: Validate migration[/bold blue]",
            border_style="blue",
        ))

        # For test-single, also validate checkpoint history
        sample_id = threads[0].thread_id if test_single and threads else None
        validation = await migrator.validate_migration(
            check_history=bool(sample_id),
            sample_thread_id=sample_id,
        )

        table = Table(title="Validation")
        table.add_column("Metric", style="cyan")
        table.add_column("Source", justify="right", style="magenta")
        table.add_column("Target", justify="right", style="magenta")

        table.add_row(
            "Threads",
            str(validation["source_count"]),
            str(validation["target_count"]),
        )
        if "history_source" in validation:
            table.add_row(
                f"Checkpoints ({sample_id[:8]}...)",
                str(validation["history_source"]),
                str(validation["history_target"]),
            )

        console.print(table)

        if validation["difference"] <= 0:
            console.print("[green]✓ Thread count validated![/green]")
        else:
            console.print(f"[yellow]⚠ Target has {validation['difference']} fewer threads[/yellow]")

        if "history_source" in validation:
            if validation["history_source"] == validation["history_target"]:
                console.print("[green]✓ Checkpoint history validated![/green]")
            else:
                diff = validation["history_source"] - validation["history_target"]
                console.print(f"[yellow]⚠ History mismatch: {diff} checkpoints missing[/yellow]")


async def run_validate(
    source_url: str,
    target_url: str,
    api_key: str,
) -> None:
    """Validate migration by comparing thread counts."""
    console.print(Panel.fit(
        "[bold blue]Validating migration[/bold blue]",
        border_style="blue",
    ))

    async with ThreadMigrator(
        source_url=source_url,
        target_url=target_url,
        api_key=api_key,
    ) as migrator:
        validation = await migrator.validate_migration()

        table = Table(title="Source vs Target")
        table.add_column("Deployment", style="cyan")
        table.add_column("Threads", justify="right", style="magenta")
        table.add_column("Status", style="green")

        table.add_row("Source", str(validation["source_count"]), "✓")
        status = "✓" if validation["difference"] <= 0 else "⚠"
        table.add_row("Target", str(validation["target_count"]), status)

        console.print(table)

        if validation["difference"] <= 0:
            console.print("[green]✓ Migration validated successfully![/green]")
        else:
            console.print(f"[yellow]⚠ Target has {validation['difference']} fewer threads[/yellow]")


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="LangGraph Threads Export Tool",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export to JSON
  python migrate_threads.py --source-url https://my.langgraph.app --export-json backup.json

  # Export to PostgreSQL
  python migrate_threads.py --source-url https://my.langgraph.app --export-postgres

  # Full migration
  python migrate_threads.py --source-url https://prod.langgraph.app --target-url https://dev.langgraph.app --full

  # Import from JSON
  python migrate_threads.py --target-url https://dev.langgraph.app --import-json backup.json
        """
    )

    parser.add_argument("--source-url", help="Source LangGraph deployment URL")
    parser.add_argument("--target-url", help="Target LangGraph deployment URL")
    parser.add_argument("--api-key", help="LangSmith API key (or set LANGSMITH_API_KEY)")
    parser.add_argument("--database-url", help="PostgreSQL URL (or set DATABASE_URL)")

    parser.add_argument("--export-json", metavar="FILE", help="Export to JSON file")
    parser.add_argument("--export-postgres", action="store_true", help="Export to PostgreSQL")
    parser.add_argument("--import-json", metavar="FILE", help="Import from JSON file")
    parser.add_argument("--full", action="store_true", help="Full migration (export + import + validate)")
    parser.add_argument("--validate", action="store_true", help="Validate migration")

    parser.add_argument("--backup-file", default="threads_backup.json", help="Backup file path")
    parser.add_argument("--dry-run", action="store_true", help="Simulation mode")
    parser.add_argument("--test-single", action="store_true", help="Test with single thread")

    args = parser.parse_args()

    # Load from environment if not provided
    api_key = args.api_key or os.getenv("LANGSMITH_API_KEY")
    database_url = args.database_url or os.getenv("DATABASE_URL")

    if not api_key:
        console.print("[red]✗ LANGSMITH_API_KEY required[/red]")
        sys.exit(1)

    # Display header
    console.print(Panel.fit(
        "[bold magenta]🔄 LangGraph Threads Export Tool[/bold magenta]",
        border_style="magenta",
    ))

    if args.test_single:
        console.print("[yellow]⚠ TEST MODE: Single thread only[/yellow]\n")

    # Run appropriate command
    try:
        if args.export_json:
            if not args.source_url:
                console.print("[red]✗ --source-url required[/red]")
                sys.exit(1)
            asyncio.run(run_export_json(
                args.source_url, api_key, args.export_json, args.test_single
            ))

        elif args.export_postgres:
            if not args.source_url:
                console.print("[red]✗ --source-url required[/red]")
                sys.exit(1)
            if not database_url:
                console.print("[red]✗ --database-url or DATABASE_URL required[/red]")
                sys.exit(1)
            asyncio.run(run_export_postgres(
                args.source_url, api_key, database_url, args.backup_file, args.test_single
            ))

        elif args.import_json:
            if not args.target_url:
                console.print("[red]✗ --target-url required[/red]")
                sys.exit(1)
            asyncio.run(run_import_json(
                args.target_url, api_key, args.import_json, args.dry_run
            ))

        elif args.full:
            if not args.source_url or not args.target_url:
                console.print("[red]✗ Both --source-url and --target-url required[/red]")
                sys.exit(1)
            asyncio.run(run_full_migration(
                args.source_url, args.target_url, api_key,
                args.backup_file, args.dry_run, args.test_single
            ))

        elif args.validate:
            if not args.source_url or not args.target_url:
                console.print("[red]✗ Both --source-url and --target-url required[/red]")
                sys.exit(1)
            asyncio.run(run_validate(args.source_url, args.target_url, api_key))

        else:
            parser.print_help()
            console.print("\n[yellow]⚠ Specify an action[/yellow]")
            sys.exit(1)

    except KeyboardInterrupt:
        console.print("\n[yellow]⚠ Interrupted[/yellow]")
        sys.exit(1)
    except Exception as e:
        console.print(f"\n[red]✗ Error: {e}[/red]")
        import traceback
        console.print(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
