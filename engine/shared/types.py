"""
Shared type definitions for the Persistent Context Engine.
THIS FILE IS THE CONTRACT. All 3 modules depend on these types.
Do NOT modify without all 3 teammates agreeing.
"""
from typing import TypedDict, Optional, Literal


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
    # 'from'/'to' in JSON, accessed via dict.get('from')
    incident_id: str
    trigger: str
    action: str
    target: str
    outcome: str


# --- Normalized Event (internal representation) ---
class NormalizedEvent(TypedDict):
    id: str                       # deterministic hash of raw event
    ts: str                       # ISO timestamp
    kind: str                     # deploy/log/metric/trace/topology/incident_signal/remediation
    canonical_service: str        # resolved through alias registry
    raw_service: str              # original name from telemetry
    data: dict                    # full raw event payload
    trace_id: Optional[str]
    incident_id: Optional[str]


# --- Benchmark output types (MUST match spec exactly) ---
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


# --- Memory Graph types ---
class MemoryEdge(TypedDict):
    source_event_id: str
    target_event_id: str
    relationship: str             # caused_by, co_occurred, remediated_by, trace_linked, preceded
    confidence: float             # 0.0 to 1.0
    evidence: str                 # human-readable reason
    timestamp: str


class IncidentFingerprint(TypedDict):
    family_id: str
    pattern: list[str]            # ordered event kinds (topology-independent)
    services_involved: int        # count, NOT names
    typical_remediation: str
    occurrences: int
