"""
Base Exporter Class

Abstract base class for all exporters.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional


@dataclass
class ThreadData:
    """
    Data structure for exported thread.

    Contains all thread information including metadata,
    values, and checkpoint history.
    """

    thread_id: str
    metadata: Dict[str, Any] = field(default_factory=dict)
    values: Dict[str, Any] = field(default_factory=dict)
    history: List[Dict[str, Any]] = field(default_factory=list)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "thread_id": self.thread_id,
            "metadata": self.metadata,
            "values": self.values,
            "history": self.history,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ThreadData":
        """Create from dictionary."""
        return cls(
            thread_id=data.get("thread_id", ""),
            metadata=data.get("metadata", {}),
            values=data.get("values", {}),
            history=data.get("history", []),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
        )


@dataclass
class ExportStats:
    """Statistics for export operation."""

    threads_exported: int = 0
    checkpoints_exported: int = 0
    errors: int = 0
    start_time: datetime = field(default_factory=datetime.now)
    end_time: Optional[datetime] = None

    @property
    def duration_seconds(self) -> float:
        """Get duration in seconds."""
        end = self.end_time or datetime.now()
        return (end - self.start_time).total_seconds()

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "threads_exported": self.threads_exported,
            "checkpoints_exported": self.checkpoints_exported,
            "errors": self.errors,
            "duration_seconds": self.duration_seconds,
        }


class BaseExporter(ABC):
    """
    Abstract base class for thread exporters.

    Subclasses must implement:
    - connect(): Establish connection (if needed)
    - export_thread(): Export a single thread
    - finalize(): Finalize export (save file, commit, etc.)
    - close(): Clean up resources
    - get_stats(): Return export statistics
    """

    def __init__(self, source_url: str):
        """
        Initialize exporter.

        Args:
            source_url: Source LangGraph deployment URL
        """
        self.source_url = source_url
        self.stats = ExportStats()

    @abstractmethod
    async def connect(self) -> None:
        """
        Establish connection (if needed).

        For file-based exporters, this might be a no-op.
        For database exporters, this establishes the connection.
        """
        pass

    @abstractmethod
    async def export_thread(self, thread: ThreadData) -> None:
        """
        Export a single thread.

        Args:
            thread: Thread data to export
        """
        pass

    @abstractmethod
    async def finalize(self) -> None:
        """
        Finalize the export.

        For file-based exporters, this saves the file.
        For database exporters, this might commit the transaction.
        """
        pass

    @abstractmethod
    async def close(self) -> None:
        """Clean up resources."""
        pass

    def get_stats(self) -> ExportStats:
        """Get export statistics."""
        self.stats.end_time = datetime.now()
        return self.stats
