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
    Classify a metric name into a category using token scoring.

    Returns one of: 'latency', 'errors', 'resource', 'traffic', 'unknown'.
    Handles proprietary/nested formats (e.g. 'com.aws.dynamodb.read.latency').
    """
    if not metric_name:
        return "unknown"

    # 1. Exact match fast path
    if metric_name in _METRIC_NAME_TO_CATEGORY:
        return _METRIC_NAME_TO_CATEGORY[metric_name]

    # 2. Tokenize the metric name (split by dot, underscore, dash)
    import re
    # Convert camelCase to snake_case spaces, then split by non-alphanumeric
    spaced = re.sub(r'([A-Z])', r' \1', metric_name).lower()
    tokens = set(re.split(r'[^a-z0-9]+', spaced))

    # 3. Score categories based on token presence
    scores = {"latency": 0, "errors": 0, "resource": 0, "traffic": 0}
    
    # Keyword weights
    latency_kws = {"latency", "duration", "response", "time", "p99", "p95", "p50", "timer"}
    error_kws = {"error", "errors", "timeout", "fail", "failure", "5xx", "4xx", "retry", "exception", "dropped"}
    resource_kws = {"cpu", "memory", "mem", "disk", "gc", "thread", "heap", "alloc", "io", "bytes"}
    traffic_kws = {"rps", "throughput", "request", "requests", "qps", "queue", "connection", "connections"}

    for token in tokens:
        if not token: continue
        if token in latency_kws: scores["latency"] += 2
        if token in error_kws: scores["errors"] += 2
        if token in resource_kws: scores["resource"] += 2
        if token in traffic_kws: scores["traffic"] += 2
        
        # Partial matches (fallback)
        if "lat" in token or "dur" in token: scores["latency"] += 1
        if "err" in token or "fail" in token: scores["errors"] += 1
        if "mem" in token or "cpu" in token: scores["resource"] += 1
        if "req" in token or "conn" in token: scores["traffic"] += 1

    # 4. Return the highest scoring category (if any score > 0)
    best_category = max(scores, key=scores.get)
    if scores[best_category] > 0:
        return best_category

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
# Event Parsing & Normalization Utilities
# ----------------------------------------------------------------

REQUIRED_FIELDS = {"ts", "kind"}
VALID_KINDS = {"deploy", "log", "metric", "trace", "topology", "incident_signal", "remediation"}

def flatten_dict(d: dict, parent_key: str = '', sep: str = '.') -> dict:
    """
    Dynamically flatten arbitrarily nested JSON into dot-notation.
    Example: {'jvm': {'gc': {'pause': 500}}} -> {'jvm.gc.pause': 500}
    """
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        else:
            items.append((new_key, v))
    return dict(items)

def parse_event(raw: dict) -> dict:
    """
    Parse and validate a raw event dict.
    Flattens nested data payloads automatically.
    """
    for field in REQUIRED_FIELDS:
        if field not in raw:
            raise ValueError(f"Missing required field: {field}")

    # Dynamically flatten any nested "data" or "metrics" objects
    if "data" in raw and isinstance(raw["data"], dict):
        raw["data"] = flatten_dict(raw["data"])

    kind = raw["kind"]
    if kind not in VALID_KINDS:
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
