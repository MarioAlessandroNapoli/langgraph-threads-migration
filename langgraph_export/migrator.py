"""
Thread Migrator

Main class for orchestrating thread export and migration operations.
"""

import asyncio
from typing import Any, Callable, Dict, List, Optional

from langgraph_sdk.errors import ConflictError, APIStatusError

from langgraph_export.client import LangGraphClient
from langgraph_export.exporters.base import BaseExporter, ExportStats, ThreadData
from langgraph_export.exporters.json_exporter import JSONExporter
from langgraph_export.exporters.postgres_exporter import PostgresExporter


class ThreadMigrator:
    """
    Thread migration and export manager.

    Handles fetching threads from source, exporting to various
    destinations, and optionally importing to a target deployment.
    """

    def __init__(
        self,
        source_url: Optional[str] = None,
        target_url: Optional[str] = None,
        api_key: str = "",
        rate_limit_delay: float = 0.2,
    ):
        """
        Initialize the migrator.

        Args:
            source_url: Source LangGraph deployment URL
            target_url: Target LangGraph deployment URL (for migration)
            api_key: LangSmith API key
            rate_limit_delay: Delay between API calls (seconds)
        """
        self.source_url = source_url
        self.target_url = target_url
        self.api_key = api_key
        self.rate_limit_delay = rate_limit_delay

        self._source_client: Optional[LangGraphClient] = None
        self._target_client: Optional[LangGraphClient] = None
        self._exporters: List[BaseExporter] = []

    async def __aenter__(self) -> "ThreadMigrator":
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()

    async def connect(self) -> None:
        """Initialize clients."""
        if self.source_url:
            self._source_client = LangGraphClient(self.source_url, self.api_key)
        if self.target_url:
            self._target_client = LangGraphClient(self.target_url, self.api_key)

    async def close(self) -> None:
        """Close all clients and exporters."""
        if self._source_client:
            await self._source_client.close()
        if self._target_client:
            await self._target_client.close()
        for exporter in self._exporters:
            await exporter.close()

    def add_json_exporter(self, output_file: str = "threads_backup.json") -> JSONExporter:
        """
        Add JSON file exporter.

        Args:
            output_file: Path to output JSON file

        Returns:
            The created exporter
        """
        exporter = JSONExporter(self.source_url or "", output_file)
        self._exporters.append(exporter)
        return exporter

    def add_postgres_exporter(self, database_url: str) -> PostgresExporter:
        """
        Add PostgreSQL exporter.

        Args:
            database_url: PostgreSQL connection URL

        Returns:
            The created exporter
        """
        exporter = PostgresExporter(self.source_url or "", database_url)
        self._exporters.append(exporter)
        return exporter

    async def fetch_all_threads(
        self,
        limit: Optional[int] = None,
        progress_callback: Optional[Callable[[int, str], None]] = None,
    ) -> List[ThreadData]:
        """
        Fetch all threads from source deployment.

        Args:
            limit: Maximum number of threads to fetch (None = all)
            progress_callback: Optional callback(count, message) for progress updates

        Returns:
            List of ThreadData objects
        """
        if not self._source_client:
            raise RuntimeError("Source URL not configured")

        all_threads: List[ThreadData] = []
        offset = 0
        batch_size = 100 if limit is None else min(limit, 100)

        # Fetch thread list
        while True:
            if progress_callback:
                progress_callback(len(all_threads), f"Fetching threads (offset={offset})...")

            threads = await self._source_client.search_threads(
                limit=batch_size,
                offset=offset,
            )

            if not threads:
                break

            for thread_summary in threads:
                thread_id = thread_summary.get("thread_id")
                if not thread_id:
                    continue

                try:
                    # Get full thread details
                    details = await self._source_client.get_thread(thread_id)
                    history = await self._source_client.get_thread_history(thread_id)

                    thread_data = ThreadData(
                        thread_id=thread_id,
                        metadata=details.get("metadata", {}),
                        values=details.get("values", {}),
                        history=history,
                        created_at=details.get("created_at"),
                        updated_at=details.get("updated_at"),
                    )
                    all_threads.append(thread_data)

                    if progress_callback:
                        progress_callback(
                            len(all_threads),
                            f"Fetched thread {thread_id[:8]}... ({len(history)} checkpoints)"
                        )

                except Exception as e:
                    if progress_callback:
                        progress_callback(len(all_threads), f"Error: {thread_id}: {e}")
                    continue

                await asyncio.sleep(self.rate_limit_delay)

                # Check limit
                if limit and len(all_threads) >= limit:
                    return all_threads

            offset += len(threads)

            # No more pages
            if len(threads) < batch_size:
                break

            await asyncio.sleep(self.rate_limit_delay)

        return all_threads

    async def export_threads(
        self,
        threads: Optional[List[ThreadData]] = None,
        limit: Optional[int] = None,
        progress_callback: Optional[Callable[[int, str], None]] = None,
    ) -> ExportStats:
        """
        Export threads to all configured exporters.

        Args:
            threads: Pre-fetched threads (if None, fetches from source)
            limit: Maximum number of threads to export
            progress_callback: Optional callback for progress updates

        Returns:
            Combined export statistics
        """
        # Fetch threads if not provided
        if threads is None:
            threads = await self.fetch_all_threads(limit, progress_callback)

        # Connect all exporters
        for exporter in self._exporters:
            await exporter.connect()

        # Export each thread
        total_checkpoints = 0
        for i, thread in enumerate(threads):
            for exporter in self._exporters:
                await exporter.export_thread(thread)
            total_checkpoints += len(thread.history)

            if progress_callback:
                progress_callback(i + 1, f"Exported {thread.thread_id[:8]}...")

        # Finalize all exporters
        for exporter in self._exporters:
            await exporter.finalize()

        # Return combined stats
        stats = ExportStats(
            threads_exported=len(threads),
            checkpoints_exported=total_checkpoints,
        )
        return stats

    async def import_threads(
        self,
        threads: List[ThreadData],
        dry_run: bool = False,
        progress_callback: Optional[Callable[[int, str], None]] = None,
    ) -> Dict[str, int]:
        """
        Import threads to target deployment.

        Args:
            threads: Threads to import
            dry_run: If True, don't actually create threads
            progress_callback: Optional callback for progress updates

        Returns:
            Dictionary with created, skipped, failed counts
        """
        if not self._target_client:
            raise RuntimeError("Target URL not configured")

        results = {"created": 0, "skipped": 0, "failed": 0}

        for i, thread in enumerate(threads):
            try:
                if dry_run:
                    results["skipped"] += 1
                    if progress_callback:
                        progress_callback(i + 1, f"[DRY-RUN] Would create {thread.thread_id[:8]}...")
                    continue

                # Create thread
                await self._target_client.create_thread(
                    thread_id=thread.thread_id,
                    metadata=thread.metadata,
                )

                # Update state if available
                if thread.values:
                    await self._target_client.update_thread_state(
                        thread_id=thread.thread_id,
                        values=thread.values,
                    )

                results["created"] += 1
                if progress_callback:
                    progress_callback(i + 1, f"Created {thread.thread_id[:8]}...")

            except ConflictError:
                results["skipped"] += 1
                if progress_callback:
                    progress_callback(i + 1, f"Skipped (exists) {thread.thread_id[:8]}...")

            except APIStatusError as e:
                results["failed"] += 1
                if progress_callback:
                    progress_callback(i + 1, f"Failed {thread.thread_id[:8]}: {e}")

            except Exception as e:
                results["failed"] += 1
                if progress_callback:
                    progress_callback(i + 1, f"Error {thread.thread_id[:8]}: {e}")

            await asyncio.sleep(self.rate_limit_delay)

        return results

    async def validate_migration(self) -> Dict[str, int]:
        """
        Compare thread counts between source and target.

        Returns:
            Dictionary with source_count, target_count
        """
        source_count = 0
        target_count = 0

        if self._source_client:
            threads = await self._source_client.search_threads(limit=10000)
            source_count = len(threads)

        if self._target_client:
            threads = await self._target_client.search_threads(limit=10000)
            target_count = len(threads)

        return {
            "source_count": source_count,
            "target_count": target_count,
            "difference": source_count - target_count,
        }
