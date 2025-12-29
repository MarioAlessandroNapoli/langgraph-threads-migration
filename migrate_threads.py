#!/usr/bin/env python3
"""
LangGraph Threads Export Tool

Export threads, checkpoints, and conversation history from LangGraph Cloud deployments.
Save to JSON file, PostgreSQL database, or migrate to another deployment.

Usage:
    # Export threads to JSON file
    python migrate_threads.py --source-url <URL> --export-json threads.json

    # Export threads to PostgreSQL database
    python migrate_threads.py --source-url <URL> --export-postgres

    # Migrate threads between deployments
    python migrate_threads.py --source-url <SOURCE> --target-url <TARGET> --migrate

    # Full migration (export + import + validate)
    python migrate_threads.py --source-url <SOURCE> --target-url <TARGET> --full

Requirements:
    pip install langgraph-sdk rich python-dotenv asyncpg

Configuration:
    Set LANGSMITH_API_KEY in .env file or pass via --api-key argument.
    Set DATABASE_URL in .env file for PostgreSQL export.
"""

import argparse
import asyncio
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from langgraph_sdk import get_client
from langgraph_sdk.errors import ConflictError, APIStatusError
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table
from rich.panel import Panel

# Load environment variables from .env
load_dotenv()

console = Console()

# PostgreSQL support (optional)
try:
    import asyncpg
    POSTGRES_AVAILABLE = True
except ImportError:
    POSTGRES_AVAILABLE = False


class PostgresExporter:
    """Export threads to PostgreSQL database."""

    def __init__(self, database_url: str):
        self.database_url = database_url
        self.pool = None

    async def connect(self):
        """Create connection pool."""
        self.pool = await asyncpg.create_pool(self.database_url)
        await self._create_tables()

    async def close(self):
        """Close connection pool."""
        if self.pool:
            await self.pool.close()

    async def _create_tables(self):
        """Create tables if they don't exist."""
        async with self.pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS langgraph_threads (
                    id SERIAL PRIMARY KEY,
                    thread_id VARCHAR(255) UNIQUE NOT NULL,
                    metadata JSONB DEFAULT '{}',
                    values JSONB DEFAULT '{}',
                    created_at TIMESTAMP,
                    updated_at TIMESTAMP,
                    source_url TEXT,
                    exported_at TIMESTAMP DEFAULT NOW()
                );

                CREATE TABLE IF NOT EXISTS langgraph_checkpoints (
                    id SERIAL PRIMARY KEY,
                    thread_id VARCHAR(255) NOT NULL REFERENCES langgraph_threads(thread_id) ON DELETE CASCADE,
                    checkpoint_id VARCHAR(255),
                    parent_checkpoint_id VARCHAR(255),
                    checkpoint_data JSONB DEFAULT '{}',
                    values JSONB DEFAULT '{}',
                    metadata JSONB DEFAULT '{}',
                    created_at TIMESTAMP,
                    UNIQUE(thread_id, checkpoint_id)
                );

                CREATE INDEX IF NOT EXISTS idx_threads_thread_id ON langgraph_threads(thread_id);
                CREATE INDEX IF NOT EXISTS idx_threads_metadata ON langgraph_threads USING GIN(metadata);
                CREATE INDEX IF NOT EXISTS idx_checkpoints_thread_id ON langgraph_checkpoints(thread_id);
            """)

    async def export_thread(self, thread_data: Dict[str, Any], source_url: str):
        """Export a single thread to PostgreSQL."""
        async with self.pool.acquire() as conn:
            # Insert thread
            await conn.execute("""
                INSERT INTO langgraph_threads (thread_id, metadata, values, created_at, updated_at, source_url)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (thread_id) DO UPDATE SET
                    metadata = EXCLUDED.metadata,
                    values = EXCLUDED.values,
                    updated_at = EXCLUDED.updated_at,
                    exported_at = NOW()
            """,
                thread_data.get("thread_id"),
                json.dumps(thread_data.get("metadata", {})),
                json.dumps(thread_data.get("values", {})),
                thread_data.get("created_at"),
                thread_data.get("updated_at"),
                source_url
            )

            # Insert checkpoints from history
            for checkpoint in thread_data.get("history", []):
                await conn.execute("""
                    INSERT INTO langgraph_checkpoints
                    (thread_id, checkpoint_id, parent_checkpoint_id, checkpoint_data, values, metadata, created_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                    ON CONFLICT (thread_id, checkpoint_id) DO UPDATE SET
                        checkpoint_data = EXCLUDED.checkpoint_data,
                        values = EXCLUDED.values,
                        metadata = EXCLUDED.metadata
                """,
                    thread_data.get("thread_id"),
                    checkpoint.get("checkpoint_id"),
                    checkpoint.get("parent_checkpoint_id"),
                    json.dumps(checkpoint.get("checkpoint", {})),
                    json.dumps(checkpoint.get("values", {})),
                    json.dumps(checkpoint.get("metadata", {})),
                    checkpoint.get("created_at")
                )

    async def get_stats(self) -> Dict[str, int]:
        """Get export statistics."""
        async with self.pool.acquire() as conn:
            threads = await conn.fetchval("SELECT COUNT(*) FROM langgraph_threads")
            checkpoints = await conn.fetchval("SELECT COUNT(*) FROM langgraph_checkpoints")
            return {"threads": threads, "checkpoints": checkpoints}


class LangGraphClient:
    """Client for interacting with LangGraph Cloud API via the official SDK."""

    def __init__(self, base_url: str, api_key: str):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.client = get_client(url=base_url, api_key=api_key)

    async def close(self):
        """Close the HTTP client."""
        pass  # SDK handles cleanup automatically

    async def search_threads(
        self, limit: int = 100, offset: int = 0
    ) -> List[Dict[str, Any]]:
        """List all threads via /threads/search endpoint."""
        threads = await self.client.threads.search(
            limit=limit,
            offset=offset,
        )
        return threads

    async def get_thread(self, thread_id: str) -> Dict[str, Any]:
        """Get thread details."""
        thread = await self.client.threads.get(thread_id)
        return thread

    async def get_thread_history(
        self, thread_id: str, limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get full thread history."""
        history = await self.client.threads.get_history(
            thread_id,
            limit=limit,
        )
        return list(history) if history else []

    async def create_thread(
        self,
        thread_id: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Create a new thread with a specific ID."""
        thread = await self.client.threads.create(
            thread_id=thread_id,
            metadata=metadata or {},
        )
        return thread

    async def update_thread_state(
        self, thread_id: str, values: Dict[str, Any], as_node: Optional[str] = None
    ) -> Dict[str, Any]:
        """Update thread state."""
        result = await self.client.threads.update_state(
            thread_id,
            values=values,
            as_node=as_node,
        )
        return result


class ThreadMigrator:
    """Thread migration and export manager."""

    def __init__(
        self,
        source_url: Optional[str],
        target_url: Optional[str],
        api_key: str,
        backup_file: str = "threads_backup.json",
        database_url: Optional[str] = None,
        dry_run: bool = False,
        test_single: bool = False,
    ):
        self.source_client = LangGraphClient(source_url, api_key) if source_url else None
        self.target_client = LangGraphClient(target_url, api_key) if target_url else None
        self.backup_file = Path(backup_file)
        self.database_url = database_url
        self.postgres_exporter = None
        self.dry_run = dry_run
        self.test_single = test_single

    async def close(self):
        """Close HTTP clients and database connections."""
        if self.source_client:
            await self.source_client.close()
        if self.target_client:
            await self.target_client.close()
        if self.postgres_exporter:
            await self.postgres_exporter.close()

    async def export_threads(self, to_postgres: bool = False) -> List[Dict[str, Any]]:
        """Export all threads from source deployment."""
        if not self.source_client:
            console.print("[red]✗ Source URL required for export[/red]")
            return []

        if self.test_single:
            console.print(
                Panel.fit(
                    "[bold yellow]Phase 1: Export SINGLE thread from source (test mode)[/bold yellow]",
                    border_style="yellow",
                )
            )
        else:
            console.print(
                Panel.fit(
                    "[bold cyan]Phase 1: Export threads from source[/bold cyan]",
                    border_style="cyan",
                )
            )

        # Initialize PostgreSQL if needed
        if to_postgres:
            if not POSTGRES_AVAILABLE:
                console.print("[red]✗ asyncpg not installed. Run: pip install asyncpg[/red]")
                return []
            if not self.database_url:
                console.print("[red]✗ DATABASE_URL required for PostgreSQL export[/red]")
                return []

            self.postgres_exporter = PostgresExporter(self.database_url)
            await self.postgres_exporter.connect()
            console.print("[green]✓[/green] Connected to PostgreSQL")

        all_threads = []
        offset = 0
        limit = 1 if self.test_single else 100

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("[cyan]Fetching threads...", total=None)

            while True:
                result = await self.source_client.search_threads(
                    limit=limit, offset=offset
                )
                threads = result if isinstance(result, list) else []

                if not threads:
                    break

                all_threads.extend(threads)
                offset += len(threads)

                progress.update(
                    task,
                    completed=offset,
                    description=f"[cyan]Fetched {offset} threads...",
                )

                # Stop after first thread in test mode
                if self.test_single and len(all_threads) >= 1:
                    break

                # Stop if we got less than limit (no more pages)
                if len(threads) < limit:
                    break

                # Rate limiting
                await asyncio.sleep(0.3)

        console.print(f"[green]✓[/green] {len(all_threads)} threads found")

        # Enrich with details and history
        enriched_threads = []
        total_checkpoints = 0

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(
                "[cyan]Fetching thread details & checkpoints...", total=len(all_threads)
            )

            for thread_summary in all_threads:
                thread_id = thread_summary.get("thread_id")
                if not thread_id:
                    continue

                try:
                    # Get full thread details
                    thread_details = await self.source_client.get_thread(thread_id)

                    # Get thread history (checkpoints)
                    history = await self.source_client.get_thread_history(thread_id)
                    total_checkpoints += len(history)

                    thread_data = {
                        "thread_id": thread_id,
                        "metadata": thread_details.get("metadata", {}),
                        "created_at": thread_details.get("created_at"),
                        "updated_at": thread_details.get("updated_at"),
                        "history": history,
                        "values": thread_details.get("values", {}),
                    }

                    enriched_threads.append(thread_data)

                    # Export to PostgreSQL if enabled
                    if to_postgres and self.postgres_exporter:
                        await self.postgres_exporter.export_thread(
                            thread_data, self.source_client.base_url
                        )

                    progress.update(task, advance=1)
                    await asyncio.sleep(0.2)  # Rate limiting

                except Exception as e:
                    console.print(
                        f"[yellow]⚠[/yellow] Error fetching thread {thread_id}: {e}"
                    )
                    continue

        # Save to JSON file (always, as backup)
        backup_data = {
            "export_date": datetime.now().isoformat(),
            "source": self.source_client.base_url,
            "total_threads": len(enriched_threads),
            "total_checkpoints": total_checkpoints,
            "threads": enriched_threads,
        }

        self.backup_file.write_text(json.dumps(backup_data, indent=2, ensure_ascii=False))
        console.print(
            f"[green]✓[/green] JSON backup saved: [bold]{self.backup_file}[/bold]"
        )
        console.print(
            f"[green]✓[/green] Size: {self.backup_file.stat().st_size / 1024 / 1024:.2f} MB"
        )
        console.print(
            f"[green]✓[/green] Total checkpoints exported: {total_checkpoints}"
        )

        # Show PostgreSQL stats if used
        if to_postgres and self.postgres_exporter:
            stats = await self.postgres_exporter.get_stats()
            console.print(
                f"[green]✓[/green] PostgreSQL: {stats['threads']} threads, {stats['checkpoints']} checkpoints"
            )

        return enriched_threads

    async def import_threads(self, threads: Optional[List[Dict[str, Any]]] = None):
        """Import threads to target deployment."""
        if not self.target_client:
            console.print("[red]✗ Target URL required for import[/red]")
            return

        if threads is None:
            # Load from backup file
            if not self.backup_file.exists():
                console.print(
                    f"[red]✗[/red] Backup file not found: {self.backup_file}"
                )
                return

            console.print(f"[cyan]Loading from {self.backup_file}...[/cyan]")
            backup_data = json.loads(self.backup_file.read_text())
            threads = backup_data.get("threads", [])

        console.print(
            Panel.fit(
                f"[bold green]Phase 2: Import {len(threads)} threads to target[/bold green]",
                border_style="green",
            )
        )

        if self.dry_run:
            console.print("[yellow]⚠ DRY-RUN MODE: No changes will be made[/yellow]")

        created = 0
        failed = 0
        skipped = 0

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            TaskProgressColumn(),
            console=console,
        ) as progress:
            task = progress.add_task("[green]Importing threads...", total=len(threads))

            for thread_data in threads:
                thread_id = thread_data.get("thread_id")
                metadata = thread_data.get("metadata", {})
                values = thread_data.get("values", {})

                try:
                    if self.dry_run:
                        console.print(
                            f"[dim]Dry-run: Would create thread {thread_id}[/dim]"
                        )
                        skipped += 1
                    else:
                        # Create thread with same ID and metadata
                        await self.target_client.create_thread(
                            thread_id=thread_id,
                            metadata=metadata,
                        )

                        # Restore state if available
                        if values:
                            await self.target_client.update_thread_state(
                                thread_id=thread_id, values=values
                            )

                        created += 1

                    progress.update(task, advance=1)
                    await asyncio.sleep(0.2)  # Rate limiting

                except ConflictError:
                    # Thread already exists (409 Conflict)
                    skipped += 1
                    progress.update(task, advance=1)

                except APIStatusError as e:
                    console.print(
                        f"[red]✗[/red] Error creating thread {thread_id}: {e}"
                    )
                    failed += 1
                    progress.update(task, advance=1)

                except Exception as e:
                    console.print(f"[red]✗[/red] Error with thread {thread_id}: {e}")
                    failed += 1
                    progress.update(task, advance=1)

        # Summary table
        table = Table(title="Import Summary")
        table.add_column("Status", style="cyan")
        table.add_column("Count", justify="right", style="magenta")

        table.add_row("Created successfully", str(created))
        table.add_row("Already exists (skipped)", str(skipped))
        table.add_row("Errors", str(failed))
        table.add_row("Total", str(len(threads)))

        console.print(table)

    async def validate_migration(self):
        """Compare thread counts between source and target."""
        console.print(
            Panel.fit(
                "[bold blue]Phase 3: Validate migration[/bold blue]",
                border_style="blue",
            )
        )

        source_count = 0
        target_count = 0

        # Count source threads
        if self.source_client:
            try:
                source_threads = await self.source_client.search_threads(limit=1000)
                source_count = len(source_threads) if isinstance(source_threads, list) else 0
            except Exception as e:
                console.print(f"[yellow]⚠ Could not count source threads: {e}[/yellow]")

        # Count target threads
        if self.target_client:
            try:
                target_threads = await self.target_client.search_threads(limit=1000)
                target_count = len(target_threads) if isinstance(target_threads, list) else 0
            except Exception as e:
                console.print(f"[yellow]⚠ Could not count target threads: {e}[/yellow]")

        # Display results
        table = Table(title="Source vs Target Comparison")
        table.add_column("Deployment", style="cyan")
        table.add_column("Thread Count", justify="right", style="magenta")
        table.add_column("Status", style="green")

        if self.source_client:
            table.add_row("Source", str(source_count), "✓")
        if self.target_client:
            status = "✓" if target_count >= source_count else "⚠"
            table.add_row("Target", str(target_count), status)

        console.print(table)

        if target_count >= source_count:
            console.print("[green]✓ Migration validated successfully![/green]")
        else:
            console.print(
                f"[yellow]⚠ Warning: Target has {source_count - target_count} fewer threads than source[/yellow]"
            )


async def main():
    parser = argparse.ArgumentParser(
        description="LangGraph Threads Export Tool - Export threads, checkpoints, and history",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Export threads to JSON only
  python migrate_threads.py --source-url https://my-prod.langgraph.app --export-json backup.json

  # Export threads to PostgreSQL database
  python migrate_threads.py --source-url https://my-prod.langgraph.app --export-postgres

  # Full migration between deployments
  python migrate_threads.py \\
    --source-url https://my-prod.langgraph.app \\
    --target-url https://my-dev.langgraph.app \\
    --full

  # Import from JSON file to another deployment
  python migrate_threads.py --target-url https://my-dev.langgraph.app --import-json backup.json

  # Test with single thread first
  python migrate_threads.py --source-url <SRC> --target-url <TGT> --full --test-single
        """
    )
    parser.add_argument(
        "--source-url",
        help="Source LangGraph Cloud deployment URL",
    )
    parser.add_argument(
        "--target-url",
        help="Target LangGraph Cloud deployment URL",
    )
    parser.add_argument(
        "--api-key",
        help="LangSmith API key (or set LANGSMITH_API_KEY in .env)",
    )
    parser.add_argument(
        "--database-url",
        help="PostgreSQL connection URL (or set DATABASE_URL in .env)",
    )
    parser.add_argument(
        "--export-json",
        metavar="FILE",
        help="Export threads to JSON file",
    )
    parser.add_argument(
        "--export-postgres",
        action="store_true",
        help="Export threads to PostgreSQL database",
    )
    parser.add_argument(
        "--import-json",
        metavar="FILE",
        help="Import threads from JSON file",
    )
    parser.add_argument(
        "--migrate",
        action="store_true",
        help="Migrate threads from source to target deployment",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Full migration: export + import + validate",
    )
    parser.add_argument(
        "--validate",
        action="store_true",
        help="Validate only (compare source vs target)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulation mode (no changes made)",
    )
    parser.add_argument(
        "--test-single",
        action="store_true",
        help="Test with single thread only",
    )
    parser.add_argument(
        "--backup-file",
        default="threads_backup.json",
        help="Backup file path (default: threads_backup.json)",
    )

    args = parser.parse_args()

    # Load API key from .env if not provided
    api_key = args.api_key or os.getenv("LANGSMITH_API_KEY")
    if not api_key:
        console.print(
            "[red]✗ Error: LANGSMITH_API_KEY not found.[/red]\n"
            "[yellow]Use --api-key or set LANGSMITH_API_KEY in .env[/yellow]"
        )
        sys.exit(1)

    # Load database URL from .env if not provided
    database_url = args.database_url or os.getenv("DATABASE_URL")

    # Validate arguments
    if args.export_json and not args.source_url:
        console.print("[red]✗ --source-url required for export[/red]")
        sys.exit(1)

    if args.export_postgres and not args.source_url:
        console.print("[red]✗ --source-url required for export[/red]")
        sys.exit(1)

    if args.export_postgres and not database_url:
        console.print("[red]✗ --database-url or DATABASE_URL in .env required for PostgreSQL export[/red]")
        sys.exit(1)

    if args.import_json and not args.target_url:
        console.print("[red]✗ --target-url required for import[/red]")
        sys.exit(1)

    if (args.migrate or args.full) and (not args.source_url or not args.target_url):
        console.print("[red]✗ Both --source-url and --target-url required for migration[/red]")
        sys.exit(1)

    if not any([args.export_json, args.export_postgres, args.import_json, args.migrate, args.full, args.validate]):
        parser.print_help()
        console.print(
            "\n[yellow]⚠ Specify an action: --export-json, --export-postgres, --import-json, --migrate, --full, or --validate[/yellow]"
        )
        sys.exit(1)

    # Display header
    console.print(
        Panel.fit(
            "[bold magenta]🔄 LangGraph Threads Export Tool[/bold magenta]",
            border_style="magenta",
        )
    )

    if args.test_single:
        console.print("[yellow]⚠ TEST MODE: Only one thread will be processed[/yellow]\n")

    # Determine backup file
    backup_file = args.export_json or args.import_json or args.backup_file

    migrator = ThreadMigrator(
        source_url=args.source_url,
        target_url=args.target_url,
        api_key=api_key,
        backup_file=backup_file,
        database_url=database_url,
        dry_run=args.dry_run,
        test_single=args.test_single,
    )

    try:
        if args.export_json:
            # Export to JSON only
            await migrator.export_threads(to_postgres=False)

        elif args.export_postgres:
            # Export to PostgreSQL (and JSON as backup)
            await migrator.export_threads(to_postgres=True)

        elif args.import_json:
            # Import from JSON
            await migrator.import_threads()

        elif args.full:
            # Full migration
            threads = await migrator.export_threads()
            await migrator.import_threads(threads)
            await migrator.validate_migration()

        elif args.migrate:
            # Migrate without validation
            threads = await migrator.export_threads()
            await migrator.import_threads(threads)

        elif args.validate:
            # Validate only
            await migrator.validate_migration()

    except KeyboardInterrupt:
        console.print("\n[yellow]⚠ Operation interrupted by user[/yellow]")
        sys.exit(1)

    except Exception as e:
        console.print(f"\n[red]✗ Fatal error: {e}[/red]")
        import traceback
        console.print(traceback.format_exc())
        sys.exit(1)

    finally:
        await migrator.close()


if __name__ == "__main__":
    asyncio.run(main())
