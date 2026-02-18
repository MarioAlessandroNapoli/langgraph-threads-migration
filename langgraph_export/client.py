"""
LangGraph Cloud API Client

Wrapper around the official LangGraph SDK for thread operations.
"""

from typing import Any, Dict, List, Optional

from langgraph_sdk import get_client


class LangGraphClient:
    """Client for interacting with LangGraph Cloud API via the official SDK."""

    def __init__(self, base_url: str, api_key: str):
        """
        Initialize the LangGraph client.

        Args:
            base_url: LangGraph Cloud deployment URL
            api_key: LangSmith API key (Service Key recommended)
        """
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self._client = get_client(url=base_url, api_key=api_key)

    async def close(self) -> None:
        """Close the HTTP client."""
        # SDK handles cleanup automatically
        pass

    async def search_threads(
        self,
        limit: int = 100,
        offset: int = 0,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Search for threads.

        Args:
            limit: Maximum number of threads to return
            offset: Number of threads to skip
            metadata: Optional metadata filter

        Returns:
            List of thread dictionaries
        """
        kwargs = {"limit": limit, "offset": offset}
        if metadata:
            kwargs["metadata"] = metadata

        threads = await self._client.threads.search(**kwargs)
        return list(threads) if threads else []

    async def get_thread(self, thread_id: str) -> Dict[str, Any]:
        """
        Get thread details.

        Args:
            thread_id: Thread ID

        Returns:
            Thread dictionary with metadata, values, etc.
        """
        thread = await self._client.threads.get(thread_id)
        return dict(thread) if thread else {}

    async def get_thread_history(
        self,
        thread_id: str,
        limit: int = 100,
        before: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get thread history (checkpoints) — single page.

        Args:
            thread_id: Thread ID
            limit: Maximum number of checkpoints to return
            before: Checkpoint cursor for pagination

        Returns:
            List of checkpoint dictionaries (most-recent-first)
        """
        kwargs: Dict[str, Any] = {"limit": limit}
        if before:
            kwargs["before"] = before

        history = await self._client.threads.get_history(thread_id, **kwargs)
        return list(history) if history else []

    async def get_all_history(
        self,
        thread_id: str,
        limit: Optional[int] = None,
        page_size: int = 100,
    ) -> List[Dict[str, Any]]:
        """
        Get complete thread history with automatic pagination.

        Paginates using the `before` cursor until all checkpoints
        are retrieved or the optional limit is reached.

        Args:
            thread_id: Thread ID
            limit: Max total checkpoints (None = all)
            page_size: Checkpoints per API call

        Returns:
            List of all checkpoint dictionaries (most-recent-first)
        """
        all_history: List[Dict[str, Any]] = []
        before = None

        while True:
            batch_size = page_size
            if limit:
                remaining = limit - len(all_history)
                batch_size = min(page_size, remaining)

            batch = await self.get_thread_history(
                thread_id, limit=batch_size, before=before,
            )

            if not batch:
                break

            all_history.extend(batch)

            if limit and len(all_history) >= limit:
                break

            if len(batch) < batch_size:
                break

            # Cursor: use the last (oldest) checkpoint in this batch
            last = batch[-1]
            checkpoint_config = last.get("checkpoint", {})
            if not checkpoint_config:
                # Fallback: build cursor from checkpoint_id
                cp_id = last.get("checkpoint_id")
                if not cp_id:
                    break
                checkpoint_config = {"checkpoint_id": cp_id}

            before = checkpoint_config

        return all_history

    async def create_thread(
        self,
        thread_id: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Create a new thread with a specific ID.

        Args:
            thread_id: Thread ID to use
            metadata: Optional metadata to attach

        Returns:
            Created thread dictionary
        """
        thread = await self._client.threads.create(
            thread_id=thread_id,
            metadata=metadata or {},
        )
        return dict(thread) if thread else {}

    async def create_thread_with_history(
        self,
        thread_id: str,
        metadata: Optional[Dict[str, Any]] = None,
        supersteps: Optional[List[Dict[str, Any]]] = None,
        if_exists: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a new thread with pre-populated checkpoint history.

        Uses the supersteps parameter to replay state updates,
        reconstructing the full checkpoint chain on the target.

        Args:
            thread_id: Thread ID to use
            metadata: Optional metadata to attach
            supersteps: List of supersteps, each containing updates
                        with values and as_node
            if_exists: Conflict behavior ('raise' or 'do_nothing')

        Returns:
            Created thread dictionary
        """
        kwargs: Dict[str, Any] = {
            "thread_id": thread_id,
            "metadata": metadata or {},
        }
        if supersteps:
            kwargs["supersteps"] = supersteps
        if if_exists:
            kwargs["if_exists"] = if_exists

        thread = await self._client.threads.create(**kwargs)
        return dict(thread) if thread else {}

    async def update_thread_state(
        self,
        thread_id: str,
        values: Dict[str, Any],
        as_node: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Update thread state.

        Args:
            thread_id: Thread ID
            values: State values to update
            as_node: Optional node name for the update

        Returns:
            Update result dictionary
        """
        result = await self._client.threads.update_state(
            thread_id,
            values=values,
            as_node=as_node,
        )
        return dict(result) if result else {}

    async def get_thread_state(self, thread_id: str) -> Dict[str, Any]:
        """
        Get current thread state.

        Args:
            thread_id: Thread ID

        Returns:
            Current state dictionary
        """
        state = await self._client.threads.get_state(thread_id)
        return dict(state) if state else {}
