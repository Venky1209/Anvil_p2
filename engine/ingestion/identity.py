"""
Canonical Identity Resolver — The foundation of the entire PCE.

Core idea: service identity is a canonical UUID, not a name. Names are aliases.
Handles:
  - First-seen registration (name → new canonical UUID)
  - Rename events (old_name → new_name, same canonical UUID)
  - Cascading rename chains (A→B→C all share one canonical UUID)
  - Temporal name ranges (which name was active at which timestamp)
  - Behavioral role inference from trace patterns

The memory substrate NEVER sees raw service names — only canonical IDs.
"""

import uuid
from typing import Optional
from engine.ingestion.storage import DuckDBStorage


class IdentityResolver:
    """
    Resolves service names to stable canonical IDs.

    Maintains an in-memory lookup cache backed by DuckDB persistence.
    The rename test is the single most critical benchmark test —
    this class is the reason it passes.
    """

    def __init__(self, storage: DuckDBStorage):
        self.storage = storage
        # In-memory caches for O(1) lookups on hot path
        self._name_to_canonical: dict[str, str] = {}  # any_name → canonical_id
        self._canonical_to_info: dict[str, dict] = {}  # canonical_id → full info
        # Load existing identities from DuckDB on startup
        self._load_from_storage()

    def _load_from_storage(self) -> None:
        """Load all service identities from DuckDB into memory cache."""
        identities = self.storage.get_all_service_identities()
        for ident in identities:
            cid = ident['canonical_id']
            self._canonical_to_info[cid] = {
                'current_name': ident['current_name'],
                'aliases': list(ident['aliases']),
                'name_ranges': list(ident['name_ranges']),
                'behavioral_role': ident.get('behavioral_role'),
                'first_seen_ts': ident['first_seen_ts'],
                'last_seen_ts': ident['last_seen_ts'],
            }
            # Map every known alias to this canonical ID
            for alias in ident['aliases']:
                self._name_to_canonical[alias] = cid

    def resolve(self, name: str, at_time: Optional[str] = None) -> str:
        """
        Resolve a service name to its canonical ID.

        If the name has never been seen, create a new canonical identity.
        If the name is a known alias, return the existing canonical ID.

        Args:
            name: The service name (may be current or historical)
            at_time: Optional timestamp for temporal resolution

        Returns:
            The canonical UUID for this service identity
        """
        if not name:
            return ""

        # Fast path: name already known
        if name in self._name_to_canonical:
            cid = self._name_to_canonical[name]
            # Update last_seen_ts
            if at_time and cid in self._canonical_to_info:
                info = self._canonical_to_info[cid]
                if at_time > info.get('last_seen_ts', ''):
                    info['last_seen_ts'] = at_time
            return cid

        # First time seeing this name — create new canonical identity
        cid = str(uuid.uuid4())[:12]  # Short UUID for readability
        ts = at_time or "1970-01-01T00:00:00Z"
        info = {
            'current_name': name,
            'aliases': [name],
            'name_ranges': [{'name': name, 'valid_from': ts, 'valid_to': None}],
            'behavioral_role': None,
            'first_seen_ts': ts,
            'last_seen_ts': ts,
        }
        self._name_to_canonical[name] = cid
        self._canonical_to_info[cid] = info

        # Persist to DuckDB
        self.storage.upsert_service_identity(
            canonical_id=cid,
            current_name=name,
            aliases=info['aliases'],
            name_ranges=info['name_ranges'],
            first_seen_ts=ts,
            last_seen_ts=ts,
        )
        return cid

    def register_rename(self, old_name: str, new_name: str, ts: str) -> str:
        """
        Register a topology rename event.

        This is the critical operation. After this call:
          - new_name resolves to the SAME canonical ID as old_name
          - All historical data under old_name is accessible via new_name
          - The name_ranges record the validity windows

        Handles cascading renames: if A→B already happened and now B→C happens,
        C maps to A's original canonical ID.

        Args:
            old_name: The service name being retired
            new_name: The new service name
            ts: Timestamp of the rename

        Returns:
            The canonical ID (unchanged by the rename)
        """
        # Resolve old_name — this may create it if never seen
        canonical_id = self.resolve(old_name, ts)
        info = self._canonical_to_info[canonical_id]

        # Close the old name's validity range
        for nr in info['name_ranges']:
            if nr['name'] == old_name and nr['valid_to'] is None:
                nr['valid_to'] = ts

        # Add the new name
        if new_name not in info['aliases']:
            info['aliases'].append(new_name)
        info['name_ranges'].append({
            'name': new_name,
            'valid_from': ts,
            'valid_to': None,
        })
        info['current_name'] = new_name
        info['last_seen_ts'] = ts

        # Update in-memory cache
        self._name_to_canonical[new_name] = canonical_id

        # Persist to DuckDB
        self.storage.upsert_service_identity(
            canonical_id=canonical_id,
            current_name=new_name,
            aliases=info['aliases'],
            name_ranges=info['name_ranges'],
            first_seen_ts=info['first_seen_ts'],
            last_seen_ts=ts,
        )

        return canonical_id

    def get_current_name(self, canonical_id: str) -> str:
        """Get the current (latest) name for a canonical ID."""
        info = self._canonical_to_info.get(canonical_id)
        if info is None:
            return ""
        return info['current_name']

    def get_name_at_time(self, canonical_id: str, at_time: str) -> str:
        """Get the name that was active at a specific timestamp."""
        info = self._canonical_to_info.get(canonical_id)
        if info is None:
            return ""
        for nr in info['name_ranges']:
            valid_from = nr['valid_from']
            valid_to = nr['valid_to']
            if at_time >= valid_from and (valid_to is None or at_time < valid_to):
                return nr['name']
        # Fallback to current name
        return info['current_name']

    def get_all_aliases(self, canonical_id: str) -> list[str]:
        """Get all names this service has ever had."""
        info = self._canonical_to_info.get(canonical_id)
        if info is None:
            return []
        return list(info['aliases'])

    def get_name_ranges(self, canonical_id: str) -> list[dict]:
        """Get temporal name ranges for a canonical ID."""
        info = self._canonical_to_info.get(canonical_id)
        if info is None:
            return []
        return list(info['name_ranges'])

    def get_canonical_id_by_name(self, name: str) -> Optional[str]:
        """Lookup canonical ID by any name (current or historical). Returns None if unknown."""
        return self._name_to_canonical.get(name)

    def set_behavioral_role(self, canonical_id: str, role: str) -> None:
        """Set the behavioral role for a service (caller, callee, gateway, etc.)."""
        info = self._canonical_to_info.get(canonical_id)
        if info:
            info['behavioral_role'] = role
            self.storage.upsert_service_identity(
                canonical_id=canonical_id,
                current_name=info['current_name'],
                aliases=info['aliases'],
                name_ranges=info['name_ranges'],
                first_seen_ts=info['first_seen_ts'],
                last_seen_ts=info['last_seen_ts'],
                behavioral_role=role,
            )

    def get_behavioral_role(self, canonical_id: str) -> Optional[str]:
        """Get the behavioral role for a service."""
        info = self._canonical_to_info.get(canonical_id)
        if info:
            return info.get('behavioral_role')
        return None

    def all_canonical_ids(self) -> list[str]:
        """Return all known canonical IDs."""
        return list(self._canonical_to_info.keys())

    def stats(self) -> dict:
        """Quick stats for debugging."""
        total_aliases = sum(len(info['aliases']) for info in self._canonical_to_info.values())
        return {
            'total_services': len(self._canonical_to_info),
            'total_aliases': total_aliases,
            'total_renames': sum(
                len(info['name_ranges']) - 1
                for info in self._canonical_to_info.values()
            ),
        }
