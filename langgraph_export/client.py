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
    ) -> List[Dict[str, Any]]:
        """
        Get full thread history (checkpoints).

        Args:
            thread_id: Thread ID
            limit: Maximum number of checkpoints to return

        Returns:
            List of checkpoint dictionaries
        """
        history = await self._client.threads.get_history(thread_id, limit=limit)
        return list(history) if history else []

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
