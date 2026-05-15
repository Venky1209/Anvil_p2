"""
Event Parser — Validates raw events, generates deterministic IDs, classifies metrics.

Handles all 7 event kinds:
  deploy, log, metric, trace, topology, incident_signal, remediation

Metric classification enables downstream causal reasoning:
  latency, errors, resource, traffic → different anomaly detection thresholds.
"""

import json
import hashlib
from typing import Optional


# ----------------------------------------------------------------
# Metric Classification System
# ----------------------------------------------------------------

METRIC_CATEGORIES = {
    "latency": [
        "latency_ms", "latency_p50_ms", "latency_p95_ms", "latency_p99_ms",
        "response_time_ms", "duration_ms",
    ],
    "errors": [
        "error_rate", "error_count", "timeout_count", "5xx_rate",
        "4xx_rate", "failure_rate", "retry_count",
    ],
    "resource": [
        "cpu_percent", "memory_percent", "disk_io", "disk_usage",
        "gc_pause_ms", "thread_count",
    ],
    "traffic": [
        "rps", "throughput", "request_count", "qps",
        "queue_depth", "connections_active",
    ],
}

# Build reverse lookup: metric_name → category
_METRIC_NAME_TO_CATEGORY: dict[str, str] = {}
for _cat, _names in METRIC_CATEGORIES.items():
    for _name in _names:
        _METRIC_NAME_TO_CATEGORY[_name] = _cat


def classify_metric(metric_name: str) -> str:
    """
    Classify a metric name into a category.

    Returns one of: 'latency', 'errors', 'resource', 'traffic', 'unknown'.
    Uses exact match first, then substring heuristics.
    """
    if not metric_name:
        return "unknown"

    # Exact match
    if metric_name in _METRIC_NAME_TO_CATEGORY:
        return _METRIC_NAME_TO_CATEGORY[metric_name]

    # Substring heuristics
    lower = metric_name.lower()
    if any(kw in lower for kw in ("latency", "duration", "response_time", "p99", "p95", "p50")):
        return "latency"
    if any(kw in lower for kw in ("error", "timeout", "fail", "5xx", "4xx", "retry")):
        return "errors"
    if any(kw in lower for kw in ("cpu", "memory", "mem", "disk", "gc_", "thread")):
        return "resource"
    if any(kw in lower for kw in ("rps", "throughput", "request", "qps", "queue", "connection")):
        return "traffic"

    return "unknown"


def is_degradation_metric(metric_name: str) -> bool:
    """
    Check if a higher value for this metric indicates degradation.

    Latency up = bad. Error rate up = bad. CPU up = potentially bad.
    Traffic up = usually good (unless saturating).
    """
    cat = classify_metric(metric_name)
    return cat in ("latency", "errors", "resource")


# ----------------------------------------------------------------
# Event Parsing
# ----------------------------------------------------------------

REQUIRED_FIELDS = {"ts", "kind"}
VALID_KINDS = {"deploy", "log", "metric", "trace", "topology", "incident_signal", "remediation"}


def parse_event(raw: dict) -> dict:
    """
    Parse and validate a raw event dict.

    Raises ValueError if required fields are missing.
    Returns the cleaned event dict.
    """
    for field in REQUIRED_FIELDS:
        if field not in raw:
            raise ValueError(f"Missing required field: {field}")

    kind = raw["kind"]
    if kind not in VALID_KINDS:
        # Don't reject unknown kinds — the spec says "teams may anticipate more"
        pass

    return raw


def generate_event_id(event: dict) -> str:
    """
    Generate a deterministic event ID from event content.

    Uses SHA-256 hash of the canonical JSON representation.
    Truncated to 16 hex chars for readability.
    """
    content = json.dumps(event, sort_keys=True, default=str)
    return hashlib.sha256(content.encode()).hexdigest()[:16]


def parse_jsonl_stream(lines):
    """
    Yield parsed events from JSONL lines (file or iterable).

    Accepts both raw strings and pre-parsed dicts.
    Silently skips blank lines. Raises on invalid JSON.
    """
    for line in lines:
        if isinstance(line, str):
            line = line.strip()
            if not line:
                continue
            raw = json.loads(line)
        elif isinstance(line, dict):
            raw = line
        else:
            continue
        yield parse_event(raw)


def extract_services_from_trace(event: dict) -> list[str]:
    """
    Extract service names from a trace event's spans.

    Returns a list of service names found in span 'svc' fields.
    """
    spans = event.get("spans", [])
    services = []
    for span in spans:
        svc = span.get("svc", "")
        if svc:
            services.append(svc)
    return services


def extract_service_from_event(event: dict) -> str:
    """
    Extract the primary service name from any event kind.

    Handles the different field locations across event types.
    """
    kind = event.get("kind", "")

    if kind == "topology":
        # For renames, the "from" service is the primary
        return event.get("from", event.get("from_name", ""))
    elif kind == "trace":
        # Traces don't have a top-level service; use first span
        spans = event.get("spans", [])
        if spans:
            return spans[0].get("svc", "")
        return ""
    elif kind == "remediation":
        return event.get("target", event.get("service", ""))
    else:
        return event.get("service", event.get("target", ""))
