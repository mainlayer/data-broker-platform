"""
In-memory + optional JSON-file persistence for dataset metadata and data.

All write operations are thread-safe via asyncio.Lock. On startup the store
attempts to load from STORAGE_PATH if set; on every mutation it persists back
to that file so state survives process restarts during development.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from src.models import DatasetRecord

logger = logging.getLogger(__name__)

_STORAGE_PATH = os.getenv("STORAGE_PATH", "")  # e.g. /data/datasets.json


def _serialize(record: DatasetRecord) -> Dict[str, Any]:
    return json.loads(record.model_dump_json(by_alias=True))


def _deserialize(raw: Dict[str, Any]) -> DatasetRecord:
    return DatasetRecord.model_validate(raw)


class DatasetStore:
    """
    Thread-safe in-memory store for DatasetRecord objects.

    Optionally persists to a JSON file at STORAGE_PATH for development
    convenience.  Production deployments should swap this out for a real
    database (Postgres, DynamoDB, etc.).
    """

    def __init__(self) -> None:
        self._records: Dict[str, DatasetRecord] = {}
        self._lock = asyncio.Lock()
        self._loaded = False

    # ------------------------------------------------------------------
    # Persistence helpers
    # ------------------------------------------------------------------

    async def _load(self) -> None:
        if self._loaded:
            return
        self._loaded = True
        if not _STORAGE_PATH:
            return
        path = Path(_STORAGE_PATH)
        if not path.exists():
            return
        try:
            raw = json.loads(path.read_text())
            for item in raw:
                record = _deserialize(item)
                self._records[record.id] = record
            logger.info("Loaded %d datasets from %s", len(self._records), _STORAGE_PATH)
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not load storage from %s: %s", _STORAGE_PATH, exc)

    def _persist(self) -> None:
        if not _STORAGE_PATH:
            return
        path = Path(_STORAGE_PATH)
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            payload = [_serialize(r) for r in self._records.values()]
            path.write_text(json.dumps(payload, indent=2, default=str))
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not persist storage to %s: %s", _STORAGE_PATH, exc)

    # ------------------------------------------------------------------
    # CRUD
    # ------------------------------------------------------------------

    async def create(self, record: DatasetRecord) -> DatasetRecord:
        async with self._lock:
            await self._load()
            self._records[record.id] = record
            self._persist()
        return record

    async def get(self, dataset_id: str) -> Optional[DatasetRecord]:
        async with self._lock:
            await self._load()
            return self._records.get(dataset_id)

    async def list_all(
        self,
        tags: Optional[List[str]] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Tuple[List[DatasetRecord], int]:
        async with self._lock:
            await self._load()
            records = list(self._records.values())

        # Filter by tags if provided
        if tags:
            tag_set = set(tags)
            records = [r for r in records if tag_set.intersection(r.tags)]

        # Sort by created_at descending
        records.sort(key=lambda r: r.created_at, reverse=True)
        total = len(records)
        return records[offset : offset + limit], total

    async def update(self, dataset_id: str, **fields: Any) -> Optional[DatasetRecord]:
        async with self._lock:
            await self._load()
            record = self._records.get(dataset_id)
            if record is None:
                return None
            data = _serialize(record)
            for key, value in fields.items():
                if value is not None:
                    # Handle the schema alias
                    actual_key = "schema" if key == "schema_" else key
                    data[actual_key] = value
            data["updated_at"] = datetime.utcnow().isoformat()
            updated = _deserialize(data)
            self._records[dataset_id] = updated
            self._persist()
        return updated

    async def delete(self, dataset_id: str) -> bool:
        async with self._lock:
            await self._load()
            if dataset_id not in self._records:
                return False
            del self._records[dataset_id]
            self._persist()
        return True

    async def increment_stats(
        self, dataset_id: str, charge_usd: float
    ) -> None:
        """Atomically increment query counter and revenue."""
        async with self._lock:
            await self._load()
            record = self._records.get(dataset_id)
            if record is None:
                return
            data = _serialize(record)
            data["total_queries"] = data.get("total_queries", 0) + 1
            data["total_revenue_usd"] = round(
                data.get("total_revenue_usd", 0.0) + charge_usd, 6
            )
            data["updated_at"] = datetime.utcnow().isoformat()
            self._records[dataset_id] = _deserialize(data)
            self._persist()

    # ------------------------------------------------------------------
    # Query helper — simple keyword / field filter over sample_data
    # ------------------------------------------------------------------

    def filter_data(
        self,
        data: List[Dict[str, Any]],
        query: str,
        limit: int,
    ) -> List[Dict[str, Any]]:
        """
        Very lightweight in-memory filter.

        Supports two modes:
        - ``field=value`` exact match (e.g. ``city=London``)
        - Free-text substring match across all string values

        In a production build this would delegate to a real database
        or search engine.
        """
        query = query.strip()
        if not query or query == "*":
            return data[:limit]

        # Try field=value syntax first
        if "=" in query and not query.startswith("{"):
            parts = query.split("=", 1)
            field, value = parts[0].strip(), parts[1].strip().lower()
            filtered = [
                row for row in data
                if str(row.get(field, "")).lower() == value
            ]
        else:
            # Free-text substring search across all values
            q_lower = query.lower()
            filtered = [
                row for row in data
                if any(q_lower in str(v).lower() for v in row.values())
            ]

        return filtered[:limit]


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_store: Optional[DatasetStore] = None


def get_store() -> DatasetStore:
    global _store
    if _store is None:
        _store = DatasetStore()
    return _store
