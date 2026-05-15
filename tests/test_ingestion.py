"""
Comprehensive Ingestion Tests for the Persistent Context Engine.

Tests cover:
  1. Basic alias registry (backwards compat with prompt doc)
  2. Identity resolver — single rename
  3. Identity resolver — cascading rename chains (A→B→C)
  4. Normalizer — topology events
  5. Normalizer — post-rename resolution
  6. Event store — DuckDB-backed queries
  7. Causal edge detection — deploy attribution
  8. Causal edge detection — trace linkage
  9. Full ingestor pipeline — end-to-end
  10. Scenario 1 — Basic incident (no rename)
  11. Scenario 2 — The rename test (THE benchmark test)
  12. Scenario 3 — Cascade scenario (A→B→C propagation)
  13. Metric classification
  14. Metric baselines and anomaly detection
"""

import json
import sys
import os

# Ensure project root is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from engine.ingestion.alias_registry import AliasRegistry
from engine.ingestion.identity import IdentityResolver
from engine.ingestion.normalizer import EventNormalizer
from engine.ingestion.event_store import EventStore
from engine.ingestion.storage import DuckDBStorage
from engine.ingestion.causal import CausalEdgeDetector
from engine.ingestion.ingestor import Ingestor
from engine.ingestion.parser import classify_metric, generate_event_id


# ================================================================
# Test 1: Basic alias registry (backwards compat)
# ================================================================

def test_alias_registry_rename():
    """Original test from prompt doc — must still pass."""
    reg = AliasRegistry()
    reg.register_rename("payments-svc", "billing-svc", "2026-05-10T14:30:00Z")
    assert reg.resolve("billing-svc") == "payments-svc", \
        f"Expected 'payments-svc', got '{reg.resolve('billing-svc')}'"
    assert reg.resolve("payments-svc") == "payments-svc"
    assert set(reg.get_all_aliases("payments-svc")) == {"payments-svc", "billing-svc"}
    print("  ✅ test_alias_registry_rename")


# ================================================================
# Test 2: Identity resolver — single rename
# ================================================================

def test_identity_single_rename():
    """After rename, both names resolve to same canonical ID."""
    storage = DuckDBStorage(":memory:")
    identity = IdentityResolver(storage)

    cid1 = identity.resolve("payments-svc", "2026-05-10T14:00:00Z")
    cid_rename = identity.register_rename("payments-svc", "billing-svc", "2026-05-10T14:30:00Z")
    cid2 = identity.resolve("billing-svc", "2026-05-10T15:00:00Z")

    assert cid1 == cid_rename, "Rename should return same canonical ID"
    assert cid1 == cid2, "Post-rename name should resolve to same canonical ID"
    assert identity.get_current_name(cid1) == "billing-svc"
    assert set(identity.get_all_aliases(cid1)) == {"payments-svc", "billing-svc"}

    storage.close()
    print("  ✅ test_identity_single_rename")


# ================================================================
# Test 3: Identity resolver — cascading rename chains
# ================================================================

def test_identity_cascading_renames():
    """A→B→C: all three names share ONE canonical ID."""
    storage = DuckDBStorage(":memory:")
    identity = IdentityResolver(storage)

    cid_a = identity.resolve("svc-alpha", "T0")
    identity.register_rename("svc-alpha", "svc-beta", "T1")
    identity.register_rename("svc-beta", "svc-gamma", "T2")

    cid_b = identity.resolve("svc-beta", "T3")
    cid_c = identity.resolve("svc-gamma", "T3")

    assert cid_a == cid_b == cid_c, \
        f"Cascading rename failed: {cid_a} != {cid_b} != {cid_c}"
    assert identity.get_current_name(cid_a) == "svc-gamma"
    assert set(identity.get_all_aliases(cid_a)) == {"svc-alpha", "svc-beta", "svc-gamma"}

    # Temporal resolution
    assert identity.get_name_at_time(cid_a, "T0") == "svc-alpha"
    assert identity.get_name_at_time(cid_a, "T1") == "svc-beta"
    assert identity.get_name_at_time(cid_a, "T2") == "svc-gamma"

    storage.close()
    print("  ✅ test_identity_cascading_renames")


# ================================================================
# Test 4: Normalizer — topology events
# ================================================================

def test_normalizer_topology_event():
    """Topology rename event resolves to canonical ID."""
    storage = DuckDBStorage(":memory:")
    identity = IdentityResolver(storage)
    norm = EventNormalizer(identity)

    event = {
        "ts": "2026-05-10T14:30:00Z",
        "kind": "topology",
        "change": "rename",
        "from": "payments-svc",
        "to": "billing-svc",
    }
    result = norm.normalize(event)

    # canonical_service should be a UUID (canonical ID), not a name
    assert result['canonical_service'] != '', "Canonical service should not be empty"
    # The raw_service should show the rename
    assert result['raw_service'] == "payments-svc->billing-svc"

    storage.close()
    print("  ✅ test_normalizer_topology_event")


# ================================================================
# Test 5: Normalizer — post-rename resolution
# ================================================================

def test_normalizer_post_rename():
    """Events after rename resolve to same canonical ID as before."""
    storage = DuckDBStorage(":memory:")
    identity = IdentityResolver(storage)
    norm = EventNormalizer(identity)

    # Pre-rename event
    pre = norm.normalize({
        "ts": "T0", "kind": "log", "service": "payments-svc",
        "level": "info", "msg": "healthy",
    })

    # Rename
    norm.normalize({
        "ts": "T1", "kind": "topology", "change": "rename",
        "from": "payments-svc", "to": "billing-svc",
    })

    # Post-rename event
    post = norm.normalize({
        "ts": "T2", "kind": "log", "service": "billing-svc",
        "level": "error", "msg": "timeout",
    })

    assert pre['canonical_service'] == post['canonical_service'], \
        f"Post-rename canonical mismatch: {pre['canonical_service']} != {post['canonical_service']}"

    storage.close()
    print("  ✅ test_normalizer_post_rename")


# ================================================================
# Test 6: Event store — DuckDB-backed queries
# ================================================================

def test_event_store_queries():
    """Event store returns correct results for various query patterns."""
    storage = DuckDBStorage(":memory:")
    identity = IdentityResolver(storage)
    store = EventStore(storage)

    cid_a = identity.resolve("svc-a", "T1")

    e1 = {"id": generate_event_id({"ts": "T1", "kind": "deploy"}),
          "ts": "T1", "kind": "deploy", "canonical_service": cid_a,
          "raw_service": "svc-a", "data": {"ts": "T1", "kind": "deploy"},
          "trace_id": None, "incident_id": None}
    e2 = {"id": generate_event_id({"ts": "T2", "kind": "log"}),
          "ts": "T2", "kind": "log", "canonical_service": cid_a,
          "raw_service": "svc-a", "data": {"ts": "T2", "kind": "log"},
          "trace_id": "tr1", "incident_id": None}

    store.append(e1)
    store.append(e2)

    assert len(store.query_by_service(cid_a)) == 2
    assert len(store.query_by_kind("deploy")) == 1
    assert len(store.query_by_trace("tr1")) == 1
    assert store.count() == 2

    storage.close()
    print("  ✅ test_event_store_queries")


# ================================================================
# Test 7: Metric classification
# ================================================================

def test_metric_classification():
    """Metrics are correctly classified into categories."""
    assert classify_metric("latency_p99_ms") == "latency"
    assert classify_metric("error_rate") == "errors"
    assert classify_metric("cpu_percent") == "resource"
    assert classify_metric("rps") == "traffic"
    assert classify_metric("custom_metric") == "unknown"
    assert classify_metric("response_time_ms") == "latency"
    assert classify_metric("timeout_count") == "errors"
    print("  ✅ test_metric_classification")


# ================================================================
# Test 8: Metric baselines and anomaly detection
# ================================================================

def test_metric_baselines():
    """Metric baselines detect anomalies correctly."""
    storage = DuckDBStorage(":memory:")

    # Build a baseline with normal values
    for val in [100, 110, 105, 95, 102, 108, 97, 103]:
        storage.upsert_metric_baseline("svc-1", "latency_p99_ms", val, "T1")

    # Check a normal value
    is_anom, z = storage.is_metric_anomalous("svc-1", "latency_p99_ms", 105)
    assert not is_anom, f"105 should not be anomalous (z={z})"

    # Check an extremely high value
    is_anom, z = storage.is_metric_anomalous("svc-1", "latency_p99_ms", 4820)
    assert is_anom, f"4820 should be anomalous (z={z})"

    storage.close()
    print("  ✅ test_metric_baselines")


# ================================================================
# Test 9: Full ingestor pipeline — end-to-end
# ================================================================

def test_ingestor_e2e():
    """Full pipeline processes events without errors."""
    ingestor = Ingestor()

    events = [
        {"ts": "2026-05-10T14:00:00Z", "kind": "deploy", "service": "auth-svc", "version": "v1.0.0", "actor": "ci"},
        {"ts": "2026-05-10T14:05:00Z", "kind": "metric", "service": "auth-svc", "name": "latency_p99_ms", "value": 120},
        {"ts": "2026-05-10T14:10:00Z", "kind": "deploy", "service": "payments-svc", "version": "v2.14.0", "actor": "ci"},
        {"ts": "2026-05-10T14:12:00Z", "kind": "metric", "service": "payments-svc", "name": "latency_p99_ms", "value": 4820},
    ]

    stats = ingestor.ingest(events)
    assert stats['events_processed'] == 4, f"Expected 4 events, got {stats['events_processed']}"
    assert stats['errors'] == 0, f"Expected 0 errors, got {stats['errors']}"

    # Verify storage
    assert ingestor.event_store.count() == 4

    # Verify identity
    assert len(ingestor.identity.all_canonical_ids()) >= 2  # auth-svc, payments-svc

    ingestor.close()
    print("  ✅ test_ingestor_e2e")


# ================================================================
# Test 10: Scenario 1 — Basic incident (no rename)
# ================================================================

def test_scenario_basic_incident():
    """Basic incident flow: deploy → spike → error → signal → remediation."""
    ingestor = Ingestor()

    events = [
        {"ts": "2026-05-10T14:10:00Z", "kind": "deploy", "service": "payments-svc",
         "version": "v2.14.0", "actor": "ci"},
        {"ts": "2026-05-10T14:12:00Z", "kind": "metric", "service": "payments-svc",
         "name": "latency_p99_ms", "value": 4820},
        {"ts": "2026-05-10T14:12:30Z", "kind": "log", "service": "checkout-api",
         "level": "error", "msg": "timeout calling payments-svc", "trace_id": "abc123"},
        {"ts": "2026-05-10T14:13:00Z", "kind": "trace", "trace_id": "abc123",
         "spans": [{"svc": "checkout-api", "dur_ms": 5012}, {"svc": "payments-svc", "dur_ms": 4980}]},
        {"ts": "2026-05-10T14:15:00Z", "kind": "incident_signal",
         "incident_id": "INC-001", "trigger": "alert:checkout-api/error-rate>5%"},
        {"ts": "2026-05-10T14:20:00Z", "kind": "remediation", "incident_id": "INC-001",
         "action": "rollback", "target": "payments-svc", "version": "v2.13.4", "outcome": "resolved"},
    ]

    stats = ingestor.ingest(events)
    assert stats['events_processed'] == 6
    assert stats['errors'] == 0

    # Verify incident episode was created
    episode = ingestor.storage.get_incident_episode("INC-001")
    assert episode is not None, "Incident episode should exist"
    assert episode['remediation_action'] == "rollback"
    assert episode['remediation_outcome'] == "resolved"

    # Verify causal edges were created
    all_edges = ingestor.storage.query_causal_edges(min_confidence=0.0)
    assert len(all_edges) > 0, "Should have causal edges"

    # Check for deploy → metric causal edge
    deploy_edges = [e for e in all_edges if 'deploy' in e['edge_type']]
    assert len(deploy_edges) > 0, "Should have deploy attribution edge"

    ingestor.close()
    print("  ✅ test_scenario_basic_incident")


# ================================================================
# Test 11: Scenario 2 — THE RENAME TEST
# ================================================================

def test_scenario_rename():
    """
    THE critical benchmark test.

    Flow:
      1. Incident on payments-svc (deploy → spike → remediation)
      2. Rename: payments-svc → billing-svc
      3. Same incident pattern on billing-svc
      4. Verify: both incidents share same canonical ID
      5. Verify: querying by billing-svc returns payments-svc history
    """
    ingestor = Ingestor()

    # Phase 1: Incident on payments-svc
    events_phase1 = [
        {"ts": "2026-05-10T14:10:00Z", "kind": "deploy", "service": "payments-svc",
         "version": "v2.14.0", "actor": "ci"},
        {"ts": "2026-05-10T14:12:00Z", "kind": "metric", "service": "payments-svc",
         "name": "latency_p99_ms", "value": 4820},
        {"ts": "2026-05-10T14:15:00Z", "kind": "incident_signal",
         "incident_id": "INC-001", "trigger": "alert/error-rate>5%"},
        {"ts": "2026-05-10T14:20:00Z", "kind": "remediation", "incident_id": "INC-001",
         "action": "rollback", "target": "payments-svc", "version": "v2.13.4", "outcome": "resolved"},
    ]

    # Phase 2: Rename
    events_rename = [
        {"ts": "2026-05-10T15:00:00Z", "kind": "topology", "change": "rename",
         "from": "payments-svc", "to": "billing-svc"},
    ]

    # Phase 3: Same pattern on billing-svc
    events_phase3 = [
        {"ts": "2026-05-10T16:00:00Z", "kind": "deploy", "service": "billing-svc",
         "version": "v2.15.0", "actor": "ci"},
        {"ts": "2026-05-10T16:02:00Z", "kind": "metric", "service": "billing-svc",
         "name": "latency_p99_ms", "value": 5100},
        {"ts": "2026-05-10T16:05:00Z", "kind": "incident_signal",
         "incident_id": "INC-002", "trigger": "alert/error-rate>5%"},
        {"ts": "2026-05-10T16:15:00Z", "kind": "remediation", "incident_id": "INC-002",
         "action": "rollback", "target": "billing-svc", "version": "v2.14.0", "outcome": "resolved"},
    ]

    ingestor.ingest(events_phase1)
    ingestor.ingest(events_rename)
    ingestor.ingest(events_phase3)

    # CRITICAL ASSERTION 1: Both names resolve to same canonical ID
    cid_payments = ingestor.identity.get_canonical_id_by_name("payments-svc")
    cid_billing = ingestor.identity.get_canonical_id_by_name("billing-svc")
    assert cid_payments == cid_billing, \
        f"Rename failed: payments={cid_payments}, billing={cid_billing}"

    # CRITICAL ASSERTION 2: Querying by canonical ID returns ALL events
    all_events = ingestor.event_store.query_by_service(cid_payments)
    # Should include events from both payments-svc and billing-svc eras
    raw_services = {e['raw_service'] for e in all_events}
    # Topology event has raw_service "payments-svc->billing-svc"
    assert any("payments-svc" in rs for rs in raw_services), \
        f"Missing payments-svc events. Raw services: {raw_services}"
    assert any("billing-svc" in rs for rs in raw_services), \
        f"Missing billing-svc events. Raw services: {raw_services}"

    # CRITICAL ASSERTION 3: Current name is billing-svc
    current_name = ingestor.identity.get_current_name(cid_payments)
    assert current_name == "billing-svc", f"Current name should be billing-svc, got {current_name}"

    # CRITICAL ASSERTION 4: Both incidents exist
    ep1 = ingestor.storage.get_incident_episode("INC-001")
    ep2 = ingestor.storage.get_incident_episode("INC-002")
    assert ep1 is not None, "INC-001 episode should exist"
    assert ep2 is not None, "INC-002 episode should exist"

    ingestor.close()
    print("  ✅ test_scenario_rename (THE benchmark test)")


# ================================================================
# Test 12: Scenario 3 — Cascade (A→B→C propagation)
# ================================================================

def test_scenario_cascade():
    """
    Cascade scenario: deploy on A → B errors → C errors.
    Causal chain must show A → B → C.
    """
    ingestor = Ingestor()

    events = [
        # A gets a deploy
        {"ts": "2026-05-10T14:00:00Z", "kind": "deploy", "service": "service-a",
         "version": "v3.0.0", "actor": "ci"},
        # A starts showing high latency
        {"ts": "2026-05-10T14:01:00Z", "kind": "metric", "service": "service-a",
         "name": "latency_p99_ms", "value": 3000},
        # Trace shows B calling A, A is slow
        {"ts": "2026-05-10T14:02:00Z", "kind": "trace", "trace_id": "cascade-1",
         "spans": [
             {"svc": "service-b", "dur_ms": 3200},
             {"svc": "service-a", "dur_ms": 3000},
         ]},
        # B starts showing errors because A is slow
        {"ts": "2026-05-10T14:03:00Z", "kind": "log", "service": "service-b",
         "level": "error", "msg": "timeout calling service-a", "trace_id": "cascade-1"},
        # Trace shows C calling B, B is slow
        {"ts": "2026-05-10T14:04:00Z", "kind": "trace", "trace_id": "cascade-2",
         "spans": [
             {"svc": "service-c", "dur_ms": 3500},
             {"svc": "service-b", "dur_ms": 3300},
         ]},
        # C starts erroring
        {"ts": "2026-05-10T14:05:00Z", "kind": "log", "service": "service-c",
         "level": "error", "msg": "upstream failure from service-b"},
        # Incident signal fires
        {"ts": "2026-05-10T14:06:00Z", "kind": "incident_signal",
         "incident_id": "INC-CASCADE", "trigger": "alert:service-c/error-rate>10%"},
    ]

    stats = ingestor.ingest(events)
    assert stats['events_processed'] == 7
    assert stats['errors'] == 0

    # Verify causal edges exist
    edges = ingestor.storage.query_causal_edges(min_confidence=0.0)
    assert len(edges) > 0, "Should have causal edges"

    # Verify trace-based edges: A→B and B→C
    trace_edges = [e for e in edges if 'trace' in e['edge_type']]
    assert len(trace_edges) >= 2, f"Expected ≥2 trace edges, got {len(trace_edges)}"

    # Verify incident episode
    episode = ingestor.storage.get_incident_episode("INC-CASCADE")
    assert episode is not None, "Cascade incident episode should exist"

    ingestor.close()
    print("  ✅ test_scenario_cascade")


# ================================================================
# Test 13: DuckDB storage stats
# ================================================================

def test_storage_stats():
    """Storage stats returns correct counts."""
    storage = DuckDBStorage(":memory:")
    stats = storage.stats()
    assert stats['events'] == 0
    assert stats['services'] == 0
    assert stats['causal_edges'] == 0
    storage.close()
    print("  ✅ test_storage_stats")


# ================================================================
# Test 14: Full sample_events.jsonl file
# ================================================================

def test_sample_events_file():
    """Process the sample events file end-to-end."""
    fixture_path = os.path.join(os.path.dirname(__file__), 'fixtures', 'sample_events.jsonl')
    if not os.path.exists(fixture_path):
        print("  ⏭️  test_sample_events_file (fixture not found, skipping)")
        return

    ingestor = Ingestor()

    with open(fixture_path, 'r') as f:
        events = []
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))

    stats = ingestor.ingest(events)
    assert stats['errors'] == 0, f"Errors processing sample events: {stats['errors']}"
    assert stats['events_processed'] > 0

    # After processing sample_events.jsonl, payments-svc and billing-svc
    # should share the same canonical ID
    cid_p = ingestor.identity.get_canonical_id_by_name("payments-svc")
    cid_b = ingestor.identity.get_canonical_id_by_name("billing-svc")
    assert cid_p == cid_b, \
        f"Sample events: rename not working. payments={cid_p}, billing={cid_b}"

    print(f"  ✅ test_sample_events_file ({stats['events_processed']} events, "
          f"{ingestor.storage.stats()['causal_edges']} edges)")

    ingestor.close()


# ================================================================
# Test 15: Edge reinforcement and decay
# ================================================================

def test_edge_reinforcement_decay():
    """Causal edges can be reinforced and decayed."""
    storage = DuckDBStorage(":memory:")

    storage.insert_causal_edge(
        edge_id="edge-1",
        cause_event_id="evt-1",
        effect_event_id="evt-2",
        cause_canonical_id="cid-1",
        effect_canonical_id="cid-1",
        edge_type="deploy_caused_degradation",
        confidence=0.5,
        evidence={"rule": "test"},
        created_at="T1",
    )

    # Reinforce
    storage.reinforce_edge("edge-1", boost=0.2)
    edges = storage.query_causal_edges(cause_canonical_id="cid-1")
    assert len(edges) == 1
    assert abs(edges[0]['confidence'] - 0.7) < 0.01, \
        f"Expected ~0.7 after reinforcement, got {edges[0]['confidence']}"

    # Decay
    storage.decay_edge("edge-1", decay=0.1)
    edges = storage.query_causal_edges(cause_canonical_id="cid-1")
    assert abs(edges[0]['confidence'] - 0.6) < 0.01

    storage.close()
    print("  ✅ test_edge_reinforcement_decay")


# ================================================================
# Runner
# ================================================================

if __name__ == "__main__":
    print("\n🧪 Running Ingestion Tests\n")
    print("─" * 50)

    tests = [
        test_alias_registry_rename,
        test_identity_single_rename,
        test_identity_cascading_renames,
        test_normalizer_topology_event,
        test_normalizer_post_rename,
        test_event_store_queries,
        test_metric_classification,
        test_metric_baselines,
        test_storage_stats,
        test_edge_reinforcement_decay,
        test_ingestor_e2e,
        test_scenario_basic_incident,
        test_scenario_rename,
        test_scenario_cascade,
        test_sample_events_file,
    ]

    passed = 0
    failed = 0
    for test_fn in tests:
        try:
            test_fn()
            passed += 1
        except Exception as e:
            failed += 1
            print(f"  ❌ {test_fn.__name__}: {e}")
            import traceback
            traceback.print_exc()

    print("─" * 50)
    if failed == 0:
        print(f"\n🎉 ALL {passed} INGESTION TESTS PASSED\n")
    else:
        print(f"\n⚠️  {passed} passed, {failed} FAILED\n")
        sys.exit(1)
