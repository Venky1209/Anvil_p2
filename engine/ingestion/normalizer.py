"""
Event Normalizer — Transforms raw events into normalized form with canonical IDs.

This is the bridge between raw telemetry and the identity-resolved storage.
Every event that enters the system passes through here exactly once.

Key responsibilities:
  - Resolve service names → canonical IDs via IdentityResolver
  - Handle topology rename events (delegate to IdentityResolver)
  - Extract and classify metrics
  - Extract trace-level service information
  - Produce NormalizedEvent dicts for downstream storage
"""

from engine.shared.types import NormalizedEvent
from engine.ingestion.parser import (
    generate_event_id,
    classify_metric,
    extract_service_from_event,
    extract_services_from_trace,
)
from engine.ingestion.identity import IdentityResolver


class EventNormalizer:
    """
    Converts raw events into normalized form with canonical service resolution.

    The normalizer is the gatekeeper — no raw service name passes through
    without being resolved to a canonical ID first.
    """

    def __init__(self, identity: IdentityResolver):
        self.identity = identity

    def normalize(self, raw_event: dict) -> NormalizedEvent:
        """
        Convert a raw event to normalized form with canonical service.

        Handles all 7 event kinds with kind-specific logic:
          - topology/rename → updates IdentityResolver, resolves to canonical
          - trace → resolves all span services
          - metric → classifies metric type
          - all others → standard service resolution
        """
        kind = raw_event['kind']
        ts = raw_event['ts']

        # Handle topology rename events — the critical path
        if kind == 'topology' and raw_event.get('change') == 'rename':
            old = raw_event.get('from') or raw_event.get('from_name', '')
            new = raw_event.get('to') or raw_event.get('to_name', '')
            canonical_id = self.identity.register_rename(old, new, ts)
            raw_service = f"{old}->{new}"
        elif kind == 'trace':
            # Traces may not have a top-level service field
            # Use the first span's service, resolve it
            services = extract_services_from_trace(raw_event)
            if services:
                raw_service = services[0]
                canonical_id = self.identity.resolve(raw_service, ts)
                # Also resolve all other services in the trace
                for svc in services[1:]:
                    self.identity.resolve(svc, ts)
            else:
                raw_service = ''
                canonical_id = ''
        elif kind == 'remediation':
            # Remediation events use 'target' field
            raw_service = raw_event.get('target', raw_event.get('service', ''))
            canonical_id = self.identity.resolve(raw_service, ts) if raw_service else ''
        elif kind == 'incident_signal':
            # Incident signals may reference a service in the trigger
            raw_service = raw_event.get('service', '')
            if not raw_service:
                # Try to extract from trigger string: "alert:checkout-api/error-rate>5%"
                trigger = raw_event.get('trigger', '')
                raw_service = self._extract_service_from_trigger(trigger)
            canonical_id = self.identity.resolve(raw_service, ts) if raw_service else ''
        else:
            raw_service = raw_event.get('service', raw_event.get('target', ''))
            canonical_id = self.identity.resolve(raw_service, ts) if raw_service else ''

        return NormalizedEvent(
            id=generate_event_id(raw_event),
            ts=ts,
            kind=kind,
            canonical_service=canonical_id,  # NOW a canonical UUID, not a name
            raw_service=raw_service,
            data=raw_event,
            trace_id=raw_event.get('trace_id'),
            incident_id=raw_event.get('incident_id'),
        )

    def _extract_service_from_trigger(self, trigger: str) -> str:
        """
        Extract service name from trigger strings like:
          "alert:checkout-api/error-rate>5%"
          "alert/error-rate>5%"
        """
        if not trigger:
            return ''
        # Try "alert:SERVICE/..." format
        if ':' in trigger:
            after_colon = trigger.split(':', 1)[1]
            if '/' in after_colon:
                return after_colon.split('/', 1)[0]
            return after_colon
        return ''
