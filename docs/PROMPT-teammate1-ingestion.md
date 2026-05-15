# Teammate 1: Ingestion & Normalization Pipeline

## Your Mission
You own **event parsing, normalization, alias tracking, and storage**. Everything that touches raw JSONL events is yours. You are the foundation — Modules B and C read from YOUR stores.

## Branch: `feat/ingestion`

---

## Phase 0: Scaffold (Hour 0-1, ALL TOGETHER)

Everyone does this together:
```bash
# 1. Clone/init the repo
git clone <your-private-repo>
cd anvil-pce

# 2. Create the folder structure
mkdir -p engine/shared engine/ingestion engine/memory engine/compiler adapters tests demo docs

# 3. Create __init__.py files
touch engine/__init__.py engine/shared/__init__.py engine/ingestion/__init__.py
touch engine/memory/__init__.py engine/compiler/__init__.py

# 4. Create requirements.txt
echo "duckdb>=0.10.0
networkx>=3.2" > requirements.txt

# 5. Create the shared types file TOGETHER (see below)
# 6. Push to main, then branch
git checkout -b feat/ingestion
```

### Shared Types (ALL agree on this before branching)
Create `engine/shared/types.py` with these exact interfaces — this is the contract:

```python
from typing import TypedDict, Optional, Literal
from datetime import datetime

# --- Raw Event from benchmark ---
class Event(TypedDict, total=False):
    ts: str
    kind: str
    service: str
    version: str
    actor: str
    level: str
    msg: str
    trace_id: str
    name: str
    value: float
    spans: list
    change: str
    from_name: str  # 'from' is reserved in Python
    to_name: str
    incident_id: str
    trigger: str
    action: str
    target: str
    outcome: str

# --- Normalized Event ---
class NormalizedEvent(TypedDict):
    id: str
    ts: str
    kind: str
    canonical_service: str
    raw_service: str
    data: dict
    trace_id: Optional[str]
    incident_id: Optional[str]

# --- Benchmark output types ---
class CausalEdge(TypedDict):
    cause_id: str
    effect_id: str
    evidence: str
    confidence: float

class IncidentMatch(TypedDict):
    past_incident_id: str
    similarity: float
    rationale: str

class Remediation(TypedDict):
    action: str
    target: str
    historical_outcome: str
    confidence: float

class Context(TypedDict):
    related_events: list[Event]
    causal_chain: list[CausalEdge]
    similar_past_incidents: list[IncidentMatch]
    suggested_remediations: list[Remediation]
    confidence: float
    explain: str

class IncidentSignal(TypedDict):
    ts: str
    kind: str
    incident_id: str
    trigger: str

# --- Memory types ---
class MemoryEdge(TypedDict):
    source_event_id: str
    target_event_id: str
    relationship: str
    confidence: float
    evidence: str
    timestamp: str

class IncidentFingerprint(TypedDict):
    family_id: str
    pattern: list[str]
    services_involved: int
    typical_remediation: str
    occurrences: int
```

---

## Phase 1: Build Your Module (Hours 1-8)

### Step 1: `engine/ingestion/parser.py`

**What it does:** Takes raw JSONL events, validates them, routes by kind.

```python
import json
import hashlib

def parse_event(raw: dict) -> dict:
    """Parse a raw event dict, validate required fields, return cleaned dict."""
    required = ['ts', 'kind']
    for field in required:
        if field not in raw:
            raise ValueError(f"Missing required field: {field}")
    return raw

def generate_event_id(event: dict) -> str:
    """Deterministic ID from event content."""
    content = json.dumps(event, sort_keys=True)
    return hashlib.sha256(content.encode()).hexdigest()[:16]

def parse_jsonl_stream(lines):
    """Yield parsed events from JSONL lines."""
    for line in lines:
        if isinstance(line, str):
            raw = json.loads(line.strip())
        else:
            raw = line
        yield parse_event(raw)
```

### Step 2: `engine/ingestion/alias_registry.py`

**THIS IS THE MOST CRITICAL FILE.** The entire benchmark hinges on rename handling.

```python
from collections import defaultdict
from typing import Optional

class AliasRegistry:
    def __init__(self):
        # canonical_name -> list of (old_name, rename_timestamp)
        self._renames: list[tuple[str, str, str]] = []  # (old, new, ts)
        self._canonical_map: dict[str, str] = {}  # current_name -> canonical

    def register_rename(self, old_name: str, new_name: str, ts: str) -> None:
        """Record a service rename."""
        canonical = self.resolve(old_name, ts)
        self._renames.append((old_name, new_name, ts))
        self._canonical_map[new_name] = canonical
        if old_name not in self._canonical_map:
            self._canonical_map[old_name] = canonical

    def resolve(self, name: str, at_time: Optional[str] = None) -> str:
        """Resolve a service name to its canonical identity."""
        if name in self._canonical_map:
            return self._canonical_map[name]
        # First time seeing this service — it IS the canonical
        self._canonical_map[name] = name
        return name

    def get_all_aliases(self, canonical: str) -> list[str]:
        """Get all names this service has ever had."""
        aliases = set()
        for name, canon in self._canonical_map.items():
            if canon == canonical:
                aliases.add(name)
        aliases.add(canonical)
        return list(aliases)
```

### Step 3: `engine/ingestion/normalizer.py`

```python
from engine.shared.types import NormalizedEvent
from engine.ingestion.parser import generate_event_id
from engine.ingestion.alias_registry import AliasRegistry

class EventNormalizer:
    def __init__(self, alias_registry: AliasRegistry):
        self.aliases = alias_registry

    def normalize(self, raw_event: dict) -> NormalizedEvent:
        """Convert raw event to normalized form with canonical service."""
        kind = raw_event['kind']
        ts = raw_event['ts']

        # Handle topology rename events
        if kind == 'topology' and raw_event.get('change') == 'rename':
            old = raw_event.get('from') or raw_event.get('from_name', '')
            new = raw_event.get('to') or raw_event.get('to_name', '')
            self.aliases.register_rename(old, new, ts)
            raw_service = f"{old}->{new}"
            canonical = self.aliases.resolve(old, ts)
        else:
            raw_service = raw_event.get('service', raw_event.get('target', ''))
            canonical = self.aliases.resolve(raw_service, ts) if raw_service else ''

        return NormalizedEvent(
            id=generate_event_id(raw_event),
            ts=ts,
            kind=kind,
            canonical_service=canonical,
            raw_service=raw_service,
            data=raw_event,
            trace_id=raw_event.get('trace_id'),
            incident_id=raw_event.get('incident_id'),
        )
```

### Step 4: `engine/ingestion/event_store.py`

```python
from collections import defaultdict
from bisect import insort
from engine.shared.types import NormalizedEvent

class EventStore:
    def __init__(self):
        self._events: list[NormalizedEvent] = []
        self._by_service: dict[str, list[NormalizedEvent]] = defaultdict(list)
        self._by_kind: dict[str, list[NormalizedEvent]] = defaultdict(list)
        self._by_trace: dict[str, list[NormalizedEvent]] = defaultdict(list)
        self._by_incident: dict[str, list[NormalizedEvent]] = defaultdict(list)
        self._by_id: dict[str, NormalizedEvent] = {}

    def append(self, event: NormalizedEvent) -> None:
        self._events.append(event)
        self._by_id[event['id']] = event
        if event['canonical_service']:
            self._by_service[event['canonical_service']].append(event)
        self._by_kind[event['kind']].append(event)
        if event.get('trace_id'):
            self._by_trace[event['trace_id']].append(event)
        if event.get('incident_id'):
            self._by_incident[event['incident_id']].append(event)

    def get_by_id(self, event_id: str):
        return self._by_id.get(event_id)

    def query_by_service(self, canonical: str, start=None, end=None):
        return self._filter_time(self._by_service.get(canonical, []), start, end)

    def query_by_kind(self, kind: str, start=None, end=None):
        return self._filter_time(self._by_kind.get(kind, []), start, end)

    def query_by_trace(self, trace_id: str):
        return self._by_trace.get(trace_id, [])

    def query_by_incident(self, incident_id: str):
        return self._by_incident.get(incident_id, [])

    def get_all(self, start=None, end=None):
        return self._filter_time(self._events, start, end)

    def _filter_time(self, events, start, end):
        result = events
        if start:
            result = [e for e in result if e['ts'] >= start]
        if end:
            result = [e for e in result if e['ts'] <= end]
        return result
```

### Step 5: Write Tests — `tests/test_ingestion.py`

```python
from engine.ingestion.alias_registry import AliasRegistry
from engine.ingestion.normalizer import EventNormalizer
from engine.ingestion.event_store import EventStore

def test_alias_registry_rename():
    reg = AliasRegistry()
    reg.register_rename("payments-svc", "billing-svc", "2026-05-10T14:30:00Z")
    assert reg.resolve("billing-svc") == "payments-svc"
    assert reg.resolve("payments-svc") == "payments-svc"
    assert set(reg.get_all_aliases("payments-svc")) == {"payments-svc", "billing-svc"}

def test_normalizer_topology_event():
    reg = AliasRegistry()
    norm = EventNormalizer(reg)
    event = {"ts": "2026-05-10T14:30:00Z", "kind": "topology", "change": "rename",
             "from": "payments-svc", "to": "billing-svc"}
    result = norm.normalize(event)
    assert result['canonical_service'] == 'payments-svc'

def test_normalizer_post_rename():
    reg = AliasRegistry()
    norm = EventNormalizer(reg)
    # First, the rename
    norm.normalize({"ts": "T1", "kind": "topology", "change": "rename",
                    "from": "payments-svc", "to": "billing-svc"})
    # Then, an event using the new name
    result = norm.normalize({"ts": "T2", "kind": "log", "service": "billing-svc",
                             "level": "error", "msg": "timeout"})
    assert result['canonical_service'] == 'payments-svc'

def test_event_store_queries():
    store = EventStore()
    e1 = {"id": "1", "ts": "T1", "kind": "deploy", "canonical_service": "svc-a",
          "raw_service": "svc-a", "data": {}, "trace_id": None, "incident_id": None}
    e2 = {"id": "2", "ts": "T2", "kind": "log", "canonical_service": "svc-a",
          "raw_service": "svc-a", "data": {}, "trace_id": "tr1", "incident_id": None}
    store.append(e1)
    store.append(e2)
    assert len(store.query_by_service("svc-a")) == 2
    assert len(store.query_by_kind("deploy")) == 1
    assert len(store.query_by_trace("tr1")) == 1

if __name__ == "__main__":
    test_alias_registry_rename()
    test_normalizer_topology_event()
    test_normalizer_post_rename()
    test_event_store_queries()
    print("ALL INGESTION TESTS PASSED")
```

---

## Phase 2: Integration Handoff (Hour 8-10)

Your module is done when:
- [ ] `AliasRegistry` correctly resolves all renames
- [ ] `EventStore` is queryable by service, kind, trace, incident
- [ ] All tests pass
- [ ] Push to `feat/ingestion`, create PR to main

**What Module B needs from you:** `EventStore` instance and `AliasRegistry` instance
**What Module C needs from you:** Same — plus `query_by_incident()` and `get_by_id()`
