"""
Thread Migrator

Main class for orchestrating thread export and migration operations.
"""

import asyncio
import logging
import random
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
        source_api_key: Optional[str] = None,
        target_api_key: Optional[str] = None,
        rate_limit_delay: float = 0.2,
    ):
        """
        Initialize the migrator.

        Args:
            source_url: Source LangGraph deployment URL
            target_url: Target LangGraph deployment URL (for migration)
            api_key: Shared API key (fallback for source/target)
            source_api_key: API key for source deployment (overrides api_key)
            target_api_key: API key for target deployment (overrides api_key)
            rate_limit_delay: Delay between API calls (seconds)
        """
        self.source_url = source_url
        self.target_url = target_url
        self.source_api_key = source_api_key or api_key
        self.target_api_key = target_api_key or api_key
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
        """Initialize clients with their respective API keys."""
        if self.source_url:
            self._source_client = LangGraphClient(self.source_url, self.source_api_key)
        if self.target_url:
            self._target_client = LangGraphClient(self.target_url, self.target_api_key)

    async def close(self) -> None:
        """Close all clients and exporters."""
        if self._source_client:
            await self._source_client.close()
        if self._target_client:
            await self._target_client.close()
        for exporter in self._exporters:
            await exporter.close()

    @staticmethod
    async def _retry(coro_factory, max_attempts=3, base_delay=1.0, label=""):
        """
        Retry an async call with exponential backoff + jitter.

        Args:
            coro_factory: Callable that returns a new coroutine each call
            max_attempts: Maximum number of attempts
            base_delay: Base delay in seconds (doubles each retry)
            label: Label for log messages
        """
        last_error = None
        for attempt in range(max_attempts):
            try:
                return await coro_factory()
            except Exception as e:
                last_error = e
                if attempt < max_attempts - 1:
                    delay = base_delay * (2 ** attempt) * (0.7 + random.random() * 0.6)
                    logging.warning(f"Retry {attempt + 1}/{max_attempts} for {label}: {e} (wait {delay:.1f}s)")
                    await asyncio.sleep(delay)
        raise last_error

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
        metadata_filter: Optional[Dict[str, Any]] = None,
        history_limit: Optional[int] = None,
        progress_callback: Optional[Callable[[int, str], None]] = None,
    ) -> List[ThreadData]:
        """
        Fetch all threads from source deployment.

        Args:
            limit: Maximum number of threads to fetch (None = all)
            metadata_filter: Optional metadata dict for server-side filtering
            history_limit: Max checkpoints per thread (None = all)
            progress_callback: Optional callback(count, message) for progress updates

        Returns:
            List of ThreadData objects
        """
        if not self._source_client:
            raise RuntimeError("Source URL not configured")

        all_threads: List[ThreadData] = []
        offset = 0
        batch_size = 100 if limit is None else min(limit, 100)

        while True:
            if progress_callback:
                progress_callback(len(all_threads), f"Fetching threads (offset={offset})...")

            threads = await self._retry(
                lambda o=offset: self._source_client.search_threads(
                    limit=batch_size, offset=o, metadata=metadata_filter,
                ),
                label="search_threads",
            )

            if not threads:
                break

            for thread_summary in threads:
                thread_id = thread_summary.get("thread_id")
                if not thread_id:
                    continue

                try:
                    details = await self._retry(
                        lambda tid=thread_id: self._source_client.get_thread(tid),
                        label=f"get_thread({thread_id[:8]})",
                    )
                    history = await self._retry(
                        lambda tid=thread_id: self._source_client.get_all_history(
                            tid, limit=history_limit,
                        ),
                        label=f"get_all_history({thread_id[:8]})",
                    )

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
                        progress_callback(len(all_threads), f"Skipped {thread_id[:8]}: {e}")
                    continue

                await asyncio.sleep(self.rate_limit_delay)

                if limit and len(all_threads) >= limit:
                    return all_threads

            offset += len(threads)

            if len(threads) < batch_size:
                break

            await asyncio.sleep(self.rate_limit_delay)

        return all_threads

    async def export_threads(
        self,
        threads: Optional[List[ThreadData]] = None,
        limit: Optional[int] = None,
        metadata_filter: Optional[Dict[str, Any]] = None,
        history_limit: Optional[int] = None,
        progress_callback: Optional[Callable[[int, str], None]] = None,
    ) -> ExportStats:
        """
        Export threads to all configured exporters.

        Args:
            threads: Pre-fetched threads (if None, fetches from source)
            limit: Maximum number of threads to export
            metadata_filter: Optional metadata dict for server-side filtering
            history_limit: Max checkpoints per thread (None = all)
            progress_callback: Optional callback for progress updates

        Returns:
            Combined export statistics
        """
        if threads is None:
            threads = await self.fetch_all_threads(
                limit=limit,
                metadata_filter=metadata_filter,
                history_limit=history_limit,
                progress_callback=progress_callback,
            )

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

    @staticmethod
    def _compute_values_delta(
        prev_values: Dict[str, Any],
        curr_values: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Compute the delta between two checkpoint states.

        For messages (add_messages reducer): extracts only new messages by ID.
        For lists of dicts with 'id' field: extracts only new items by ID.
        For all other fields: includes only if changed.
        """
        delta: Dict[str, Any] = {}

        for key, curr_val in curr_values.items():
            prev_val = prev_values.get(key)

            if curr_val == prev_val:
                continue

            if key == "messages":
                # add_messages reducer: diff by message ID
                prev_ids = {
                    m.get("id") for m in (prev_val or []) if isinstance(m, dict)
                }
                new_msgs = [
                    m for m in (curr_val or [])
                    if isinstance(m, dict) and m.get("id") not in prev_ids
                ]
                if new_msgs:
                    delta["messages"] = new_msgs

            elif (
                isinstance(curr_val, list)
                and curr_val
                and isinstance(curr_val[0], dict)
                and "id" in curr_val[0]
            ):
                # List of dicts with ID → dedup-aware delta
                prev_ids = {
                    item.get("id")
                    for item in (prev_val or [])
                    if isinstance(item, dict)
                }
                new_items = [
                    item for item in curr_val
                    if isinstance(item, dict) and item.get("id") not in prev_ids
                ]
                if new_items:
                    delta[key] = new_items

            else:
                # Scalar or non-ID list → include if changed
                delta[key] = curr_val

        return delta

    @staticmethod
    def _history_to_supersteps(
        history: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        """
        Convert checkpoint history to supersteps format for threads.create().

        History from get_history() is most-recent-first.
        Supersteps must be chronological (oldest-first).

        Strategy:
        - If metadata.writes is available (local checkpointer), use it directly.
        - Otherwise (Cloud API), compute deltas between consecutive states.
        Only state-changing steps produce supersteps; middleware no-ops are skipped.
        """
        if not history:
            return []

        chronological = list(reversed(history))
        supersteps: List[Dict[str, Any]] = []

        # Check if writes data is available (first non-empty checkpoint)
        has_writes = any(
            isinstance((h.get("metadata") or {}).get("writes"), dict)
            for h in history
        )

        if has_writes:
            # Direct approach: use metadata.writes
            for state in chronological:
                metadata = state.get("metadata") or {}
                writes = metadata.get("writes")
                if not writes or not isinstance(writes, dict):
                    continue
                updates = []
                for node_name, node_values in writes.items():
                    if node_values is None:
                        continue
                    updates.append({"values": node_values, "as_node": node_name})
                if updates:
                    supersteps.append({"updates": updates})
        else:
            # Delta approach: compute diffs between consecutive states
            prev_values: Dict[str, Any] = {}
            for i, state in enumerate(chronological):
                curr_values = state.get("values") or {}

                if i == 0:
                    # Include initial state as first superstep
                    if curr_values:
                        supersteps.append({
                            "updates": [{
                                "values": curr_values,
                                "as_node": "__start__",
                            }],
                        })
                    prev_values = curr_values
                    continue

                # as_node = what the previous checkpoint's next field says ran
                prev_next = chronological[i - 1].get("next", [])
                as_node = prev_next[0] if prev_next else "__unknown__"

                delta = ThreadMigrator._compute_values_delta(prev_values, curr_values)

                if delta:
                    supersteps.append({
                        "updates": [{"values": delta, "as_node": as_node}],
                    })

                prev_values = curr_values

        return supersteps

    async def _import_thread_with_history(
        self,
        thread: ThreadData,
    ) -> int:
        """
        Import a single thread using supersteps to preserve checkpoint history.

        Returns the number of checkpoints imported.
        Raises on failure so caller can handle fallback.
        """
        supersteps = self._history_to_supersteps(thread.history)

        await self._target_client.create_thread_with_history(
            thread_id=thread.thread_id,
            metadata=thread.metadata,
            supersteps=supersteps if supersteps else None,
        )
        return len(supersteps)

    async def _import_thread_legacy(self, thread: ThreadData) -> None:
        """Fallback: create thread + update_state (no history preservation)."""
        await self._target_client.create_thread(
            thread_id=thread.thread_id,
            metadata=thread.metadata,
        )
        if thread.values:
            await self._target_client.update_thread_state(
                thread_id=thread.thread_id,
                values=thread.values,
            )

    async def import_threads(
        self,
        threads: List[ThreadData],
        dry_run: bool = False,
        progress_callback: Optional[Callable[[int, str], None]] = None,
    ) -> Dict[str, int]:
        """
        Import threads to target deployment.

        Tries supersteps-based import first to preserve checkpoint history.
        Falls back to legacy create+update_state if the target API
        doesn't support supersteps.

        Args:
            threads: Threads to import
            dry_run: If True, don't actually create threads
            progress_callback: Optional callback for progress updates

        Returns:
            Dictionary with created, skipped, failed, checkpoints counts
        """
        if not self._target_client:
            raise RuntimeError("Target URL not configured")

        results = {"created": 0, "skipped": 0, "failed": 0, "checkpoints": 0}
        use_legacy = False

        for i, thread in enumerate(threads):
            try:
                if dry_run:
                    supersteps = self._history_to_supersteps(thread.history)
                    results["skipped"] += 1
                    if progress_callback:
                        progress_callback(
                            i + 1,
                            f"[DRY-RUN] Would create {thread.thread_id[:8]}... "
                            f"({len(supersteps)} supersteps)"
                        )
                    continue

                if not use_legacy:
                    try:
                        checkpoints = await self._import_thread_with_history(thread)
                        results["checkpoints"] += checkpoints
                    except APIStatusError as e:
                        # supersteps not supported — switch to legacy for all remaining
                        if e.status_code in (400, 422):
                            use_legacy = True
                            if progress_callback:
                                progress_callback(
                                    i + 1,
                                    "supersteps not supported, falling back to legacy import"
                                )
                            await self._import_thread_legacy(thread)
                        else:
                            raise
                else:
                    await self._import_thread_legacy(thread)

                results["created"] += 1
                if progress_callback:
                    cp_count = len(thread.history)
                    mode = "legacy" if use_legacy else f"{cp_count} checkpoints"
                    progress_callback(i + 1, f"Created {thread.thread_id[:8]}... ({mode})")

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

    async def validate_migration(
        self,
        check_history: bool = False,
        sample_thread_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Compare thread counts between source and target.

        Optionally validates checkpoint history for a sample thread.

        Returns:
            Dictionary with source_count, target_count, and optionally
            history_source/history_target for the sample thread.
        """
        source_count = 0
        target_count = 0

        if self._source_client:
            threads = await self._source_client.search_threads(limit=10000)
            source_count = len(threads)

        if self._target_client:
            threads = await self._target_client.search_threads(limit=10000)
            target_count = len(threads)

        result: Dict[str, Any] = {
            "source_count": source_count,
            "target_count": target_count,
            "difference": source_count - target_count,
        }

        # Validate checkpoint history for a sample thread
        if check_history and sample_thread_id:
            if self._source_client:
                src_history = await self._source_client.get_all_history(sample_thread_id)
                result["history_source"] = len(src_history)
            if self._target_client:
                tgt_history = await self._target_client.get_all_history(sample_thread_id)
                result["history_target"] = len(tgt_history)

        return result
