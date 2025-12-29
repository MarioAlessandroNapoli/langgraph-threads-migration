"""
SQLAlchemy Models for LangGraph Threads Export

Database schema for storing exported threads and checkpoints.
"""

from datetime import datetime
from typing import Any, Dict, Optional

from sqlalchemy import (
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for all models."""
    pass


class Thread(Base):
    """
    Exported LangGraph thread.

    Stores thread metadata, current values, and source information.
    """

    __tablename__ = "langgraph_threads"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    thread_id: Mapped[str] = mapped_column(String(255), unique=True, nullable=False, index=True)
    metadata_: Mapped[Dict[str, Any]] = mapped_column("metadata", JSONB, default=dict)
    values: Mapped[Dict[str, Any]] = mapped_column(JSONB, default=dict)
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    updated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    source_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    exported_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)

    # Relationships
    checkpoints: Mapped[list["Checkpoint"]] = relationship(
        "Checkpoint",
        back_populates="thread",
        cascade="all, delete-orphan",
        lazy="selectin",
    )

    # Indexes
    __table_args__ = (
        Index("idx_threads_metadata", "metadata", postgresql_using="gin"),
        Index("idx_threads_source_url", "source_url"),
        Index("idx_threads_exported_at", "exported_at"),
    )

    def __repr__(self) -> str:
        return f"<Thread(thread_id={self.thread_id!r})>"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "thread_id": self.thread_id,
            "metadata": self.metadata_,
            "values": self.values,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "updated_at": self.updated_at.isoformat() if self.updated_at else None,
            "source_url": self.source_url,
            "exported_at": self.exported_at.isoformat() if self.exported_at else None,
        }


class Checkpoint(Base):
    """
    Exported LangGraph checkpoint.

    Stores checkpoint data, values, and parent relationships.
    """

    __tablename__ = "langgraph_checkpoints"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    thread_id: Mapped[str] = mapped_column(
        String(255),
        ForeignKey("langgraph_threads.thread_id", ondelete="CASCADE"),
        nullable=False,
    )
    checkpoint_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    parent_checkpoint_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    checkpoint_data: Mapped[Dict[str, Any]] = mapped_column(JSONB, default=dict)
    values: Mapped[Dict[str, Any]] = mapped_column(JSONB, default=dict)
    metadata_: Mapped[Dict[str, Any]] = mapped_column("metadata", JSONB, default=dict)
    created_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # Relationships
    thread: Mapped["Thread"] = relationship("Thread", back_populates="checkpoints")

    # Constraints and indexes
    __table_args__ = (
        UniqueConstraint("thread_id", "checkpoint_id", name="uq_thread_checkpoint"),
        Index("idx_checkpoints_thread_id", "thread_id"),
        Index("idx_checkpoints_checkpoint_id", "checkpoint_id"),
        Index("idx_checkpoints_parent", "parent_checkpoint_id"),
    )

    def __repr__(self) -> str:
        return f"<Checkpoint(thread_id={self.thread_id!r}, checkpoint_id={self.checkpoint_id!r})>"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "thread_id": self.thread_id,
            "checkpoint_id": self.checkpoint_id,
            "parent_checkpoint_id": self.parent_checkpoint_id,
            "checkpoint_data": self.checkpoint_data,
            "values": self.values,
            "metadata": self.metadata_,
            "created_at": self.created_at.isoformat() if self.created_at else None,
        }


class Database:
    """
    Database connection manager.

    Handles async SQLAlchemy engine and session creation.
    """

    def __init__(self, database_url: str):
        """
        Initialize database connection.

        Args:
            database_url: PostgreSQL connection URL
                Example: postgresql+asyncpg://user:pass@localhost/dbname
        """
        # Ensure async driver is used
        if database_url.startswith("postgresql://"):
            database_url = database_url.replace(
                "postgresql://", "postgresql+asyncpg://", 1
            )

        self.database_url = database_url
        self.engine = create_async_engine(
            database_url,
            echo=False,
            pool_size=5,
            max_overflow=10,
        )
        self.async_session = async_sessionmaker(
            self.engine,
            class_=AsyncSession,
            expire_on_commit=False,
        )

    async def create_tables(self) -> None:
        """Create all tables if they don't exist."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def drop_tables(self) -> None:
        """Drop all tables (use with caution!)."""
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)

    async def close(self) -> None:
        """Close the database connection."""
        await self.engine.dispose()

    def session(self) -> AsyncSession:
        """Get a new database session."""
        return self.async_session()
