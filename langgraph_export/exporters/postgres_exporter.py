"""
PostgreSQL Exporter

Export threads to PostgreSQL database using SQLAlchemy.
"""

from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import select, func
from sqlalchemy.dialects.postgresql import insert

from langgraph_export.exporters.base import BaseExporter, ThreadData
from langgraph_export.models import Checkpoint, Database, Thread


class PostgresExporter(BaseExporter):
    """
    Export threads to PostgreSQL database.

    Uses SQLAlchemy for database operations with proper
    schema and indexes.
    """

    def __init__(
        self,
        source_url: str,
        database_url: str,
        batch_size: int = 100,
    ):
        """
        Initialize PostgreSQL exporter.

        Args:
            source_url: Source LangGraph deployment URL
            database_url: PostgreSQL connection URL
            batch_size: Number of threads to commit at once
        """
        super().__init__(source_url)
        self.database_url = database_url
        self.batch_size = batch_size
        self._db: Optional[Database] = None
        self._pending_count = 0

    async def connect(self) -> None:
        """Establish database connection and create tables."""
        self._db = Database(self.database_url)
        await self._db.create_tables()

    async def export_thread(self, thread: ThreadData) -> None:
        """
        Export a single thread to PostgreSQL.

        Uses upsert (INSERT ... ON CONFLICT UPDATE) to handle
        re-exports gracefully.

        Args:
            thread: Thread data to export
        """
        if not self._db:
            raise RuntimeError("Database not connected. Call connect() first.")

        async with self._db.session() as session:
            try:
                # Parse datetime strings if present
                created_at = self._parse_datetime(thread.created_at)
                updated_at = self._parse_datetime(thread.updated_at)

                # Upsert thread
                stmt = insert(Thread).values(
                    thread_id=thread.thread_id,
                    metadata_=thread.metadata,
                    values=thread.values,
                    created_at=created_at,
                    updated_at=updated_at,
                    source_url=self.source_url,
                    exported_at=datetime.utcnow(),
                )
                stmt = stmt.on_conflict_do_update(
                    index_elements=["thread_id"],
                    set_={
                        "metadata_": stmt.excluded.metadata_,
                        "values": stmt.excluded.values,
                        "updated_at": stmt.excluded.updated_at,
                        "exported_at": datetime.utcnow(),
                    },
                )
                await session.execute(stmt)

                # Export checkpoints
                for checkpoint in thread.history:
                    checkpoint_created_at = self._parse_datetime(
                        checkpoint.get("created_at")
                    )

                    checkpoint_stmt = insert(Checkpoint).values(
                        thread_id=thread.thread_id,
                        checkpoint_id=checkpoint.get("checkpoint_id"),
                        parent_checkpoint_id=checkpoint.get("parent_checkpoint_id"),
                        checkpoint_data=checkpoint.get("checkpoint", {}),
                        values=checkpoint.get("values", {}),
                        metadata_=checkpoint.get("metadata", {}),
                        created_at=checkpoint_created_at,
                    )
                    checkpoint_stmt = checkpoint_stmt.on_conflict_do_update(
                        constraint="uq_thread_checkpoint",
                        set_={
                            "checkpoint_data": checkpoint_stmt.excluded.checkpoint_data,
                            "values": checkpoint_stmt.excluded.values,
                            "metadata_": checkpoint_stmt.excluded.metadata_,
                        },
                    )
                    await session.execute(checkpoint_stmt)

                await session.commit()

                self.stats.threads_exported += 1
                self.stats.checkpoints_exported += len(thread.history)

            except Exception as e:
                await session.rollback()
                self.stats.errors += 1
                raise RuntimeError(f"Failed to export thread {thread.thread_id}: {e}")

    async def finalize(self) -> None:
        """Finalize export (no-op for PostgreSQL as we commit per thread)."""
        pass

    async def close(self) -> None:
        """Close database connection."""
        if self._db:
            await self._db.close()
            self._db = None

    async def get_thread_count(self) -> int:
        """Get total number of threads in database."""
        if not self._db:
            return 0

        async with self._db.session() as session:
            result = await session.execute(select(func.count(Thread.id)))
            return result.scalar() or 0

    async def get_checkpoint_count(self) -> int:
        """Get total number of checkpoints in database."""
        if not self._db:
            return 0

        async with self._db.session() as session:
            result = await session.execute(select(func.count(Checkpoint.id)))
            return result.scalar() or 0

    async def get_database_stats(self) -> Dict[str, Any]:
        """
        Get database statistics.

        Returns:
            Dictionary with thread and checkpoint counts
        """
        return {
            "threads": await self.get_thread_count(),
            "checkpoints": await self.get_checkpoint_count(),
        }

    @staticmethod
    def _parse_datetime(value: Any) -> Optional[datetime]:
        """Parse datetime from string or return None."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            try:
                # Try ISO format first
                return datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                pass
            try:
                # Try common format
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass
        return None
