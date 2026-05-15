"""
Engine Ingestion Module — Persistent Context Engine telemetry ingestion.

Public API:
    from engine.ingestion import Ingestor

    ingestor = Ingestor()
    ingestor.ingest(events)
    store = ingestor.get_event_store()
    identity = ingestor.get_identity()
"""

from engine.ingestion.ingestor import Ingestor
from engine.ingestion.storage import DuckDBStorage
from engine.ingestion.identity import IdentityResolver
from engine.ingestion.normalizer import EventNormalizer
from engine.ingestion.event_store import EventStore
from engine.ingestion.causal import CausalEdgeDetector
from engine.ingestion.alias_registry import AliasRegistry
from engine.ingestion.parser import (
    parse_event,
    generate_event_id,
    parse_jsonl_stream,
    classify_metric,
)

__all__ = [
    'Ingestor',
    'DuckDBStorage',
    'IdentityResolver',
    'EventNormalizer',
    'EventStore',
    'CausalEdgeDetector',
    'AliasRegistry',
    'parse_event',
    'generate_event_id',
    'parse_jsonl_stream',
    'classify_metric',
]
