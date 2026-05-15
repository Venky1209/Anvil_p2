"""
Alias Registry — Backwards-compatible wrapper around IdentityResolver.

This file maintains the original AliasRegistry interface from the prompt doc
so that existing tests continue to pass, while delegating all real work
to the new IdentityResolver + DuckDB backend.

For new code, use IdentityResolver directly.
"""

from typing import Optional
from engine.ingestion.storage import DuckDBStorage
from engine.ingestion.identity import IdentityResolver


class AliasRegistry:
    """
    Backwards-compatible alias registry.

    Wraps IdentityResolver and maps canonical UUIDs back to
    the original "first name = canonical" behavior expected
    by the prompt-doc tests.
    """

    def __init__(self, storage: Optional[DuckDBStorage] = None):
        if storage is None:
            storage = DuckDBStorage(":memory:")
        self._storage = storage
        self._identity = IdentityResolver(storage)
        # Map canonical_id → first_name for backwards compat
        self._first_names: dict[str, str] = {}

    @property
    def identity(self) -> IdentityResolver:
        """Access the underlying IdentityResolver."""
        return self._identity

    def register_rename(self, old_name: str, new_name: str, ts: str) -> None:
        """Record a service rename."""
        # Ensure old_name is registered first
        cid = self._identity.resolve(old_name, ts)
        if cid not in self._first_names:
            self._first_names[cid] = old_name
        self._identity.register_rename(old_name, new_name, ts)

    def resolve(self, name: str, at_time: Optional[str] = None) -> str:
        """
        Resolve a service name to its canonical identity.

        Returns the FIRST NAME (not UUID) for backwards compatibility
        with the original test expectations.
        """
        cid = self._identity.resolve(name, at_time)
        if not cid:
            return name
        if cid not in self._first_names:
            self._first_names[cid] = name
        return self._first_names[cid]

    def get_all_aliases(self, canonical_name: str) -> list[str]:
        """Get all names this service has ever had."""
        # Find the canonical_id for this name
        cid = self._identity.get_canonical_id_by_name(canonical_name)
        if cid is None:
            return [canonical_name]
        return self._identity.get_all_aliases(cid)

    def get_canonical_id(self, name: str) -> str:
        """Get the actual canonical UUID (not the first-name shortcut)."""
        return self._identity.resolve(name)
