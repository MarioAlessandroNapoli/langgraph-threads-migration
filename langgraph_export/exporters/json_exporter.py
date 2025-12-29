"""
JSON Exporter

Export threads to a JSON file.
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from langgraph_export.exporters.base import BaseExporter, ExportStats, ThreadData


class JSONExporter(BaseExporter):
    """
    Export threads to JSON file.

    Creates a JSON file with all threads, metadata, and checkpoints.
    """

    def __init__(
        self,
        source_url: str,
        output_file: str = "threads_backup.json",
        indent: int = 2,
    ):
        """
        Initialize JSON exporter.

        Args:
            source_url: Source LangGraph deployment URL
            output_file: Path to output JSON file
            indent: JSON indentation level (default: 2)
        """
        super().__init__(source_url)
        self.output_file = Path(output_file)
        self.indent = indent
        self._threads: List[Dict[str, Any]] = []

    async def connect(self) -> None:
        """No connection needed for JSON export."""
        pass

    async def export_thread(self, thread: ThreadData) -> None:
        """
        Add thread to export buffer.

        Args:
            thread: Thread data to export
        """
        self._threads.append(thread.to_dict())
        self.stats.threads_exported += 1
        self.stats.checkpoints_exported += len(thread.history)

    async def finalize(self) -> None:
        """Save all threads to JSON file."""
        export_data = {
            "export_date": datetime.now().isoformat(),
            "source": self.source_url,
            "total_threads": len(self._threads),
            "total_checkpoints": self.stats.checkpoints_exported,
            "threads": self._threads,
        }

        # Ensure parent directory exists
        self.output_file.parent.mkdir(parents=True, exist_ok=True)

        # Write JSON file
        self.output_file.write_text(
            json.dumps(export_data, indent=self.indent, ensure_ascii=False, default=str)
        )

    async def close(self) -> None:
        """Clear internal buffer."""
        self._threads.clear()

    def get_file_size_mb(self) -> float:
        """Get output file size in MB."""
        if self.output_file.exists():
            return self.output_file.stat().st_size / 1024 / 1024
        return 0.0

    @staticmethod
    def load_threads(file_path: str) -> List[ThreadData]:
        """
        Load threads from JSON file.

        Args:
            file_path: Path to JSON file

        Returns:
            List of ThreadData objects
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        data = json.loads(path.read_text())
        threads = data.get("threads", [])

        return [ThreadData.from_dict(t) for t in threads]

    @staticmethod
    def get_export_info(file_path: str) -> Dict[str, Any]:
        """
        Get export metadata from JSON file.

        Args:
            file_path: Path to JSON file

        Returns:
            Dictionary with export date, source, and counts
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")

        data = json.loads(path.read_text())
        return {
            "export_date": data.get("export_date"),
            "source": data.get("source"),
            "total_threads": data.get("total_threads", 0),
            "total_checkpoints": data.get("total_checkpoints", 0),
            "file_size_mb": path.stat().st_size / 1024 / 1024,
        }
