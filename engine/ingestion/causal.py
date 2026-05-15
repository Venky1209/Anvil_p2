"""
Causal Edge Detector — Builds causal relationships at ingest time.

Four detection rules, all computed during ingestion (NOT at query time):

  Rule 1 — Deploy Attribution:
    deploy event → metric degradation within 5-minute window → causal edge
    Confidence: inversely proportional to number of concurrent deploys

  Rule 2 — Trace Linkage:
    trace spans reveal caller→callee relationships with explicit timing
    High confidence (0.85+) since traces are ground truth

  Rule 3 — Log-Metric Correlation:
    error log within 2 minutes of metric anomaly for same service → causal edge
    Medium confidence (0.5-0.7)

  Rule 4 — Metric Co-movement:
    Two services' metrics degrade together within a short window → correlation edge
    Low initial confidence (0.3), reinforced if pattern repeats

All edges are stored immediately — context reconstruction is a traversal, not a computation.
"""

import uuid
from typing import Optional
from engine.ingestion.storage import DuckDBStorage
from engine.ingestion.identity import IdentityResolver
from engine.ingestion.parser import classify_metric, is_degradation_metric


class CausalEdgeDetector:
    """
    Detects and stores causal relationships between events.

    Operates at ingest time. By the time reconstruct_context() is called,
    all edges are pre-built and ready for traversal.
    """

    # Time windows for causal detection (in ISO 8601 duration strings won't work,
    # so we use minute-based heuristics via string comparison on timestamps)
    DEPLOY_ATTRIBUTION_WINDOW_MINUTES = 5
    LOG_METRIC_CORRELATION_WINDOW_MINUTES = 2
    METRIC_COMOVEMENT_WINDOW_MINUTES = 3

    def __init__(self, storage: DuckDBStorage, identity: IdentityResolver):
        self.storage = storage
        self.identity = identity
        # In-memory buffers for windowed detection
        self._recent_deploys: list[dict] = []  # Recent deploy events (last 10 min)
        self._recent_metrics: list[dict] = []  # Recent metric events (last 5 min)
        self._recent_logs: list[dict] = []     # Recent error logs (last 5 min)
        # Track trace-derived dependencies for behavioral role inference
        self._trace_dependencies: dict[str, set[str]] = {}  # caller_cid → {callee_cid}
        # Track edge deduplication
        self._edge_pairs: set[tuple[str, str]] = set()

    def on_event(self, event_id: str, event: dict, canonical_id: str) -> list[dict]:
        """
        Process a newly ingested event for causal edge detection.

        Returns list of newly created edges (for downstream use).
        """
        kind = event.get("kind", "")
        new_edges = []

        if kind == "deploy":
            new_edges.extend(self._handle_deploy(event_id, event, canonical_id))
        elif kind == "metric":
            new_edges.extend(self._handle_metric(event_id, event, canonical_id))
        elif kind == "log":
            new_edges.extend(self._handle_log(event_id, event, canonical_id))
        elif kind == "trace":
            new_edges.extend(self._handle_trace(event_id, event, canonical_id))

        # Prune old entries from buffers (keep last 200 for memory efficiency)
        if len(self._recent_deploys) > 200:
            self._recent_deploys = self._recent_deploys[-100:]
        if len(self._recent_metrics) > 200:
            self._recent_metrics = self._recent_metrics[-100:]
        if len(self._recent_logs) > 200:
            self._recent_logs = self._recent_logs[-100:]

        return new_edges

    def _handle_deploy(self, event_id: str, event: dict, canonical_id: str) -> list[dict]:
        """
        Rule 1 (part 1): Record deploy, check if there are already degraded metrics.
        """
        deploy_entry = {
            'event_id': event_id,
            'canonical_id': canonical_id,
            'ts': event['ts'],
            'version': event.get('version', ''),
            'event': event,
        }
        self._recent_deploys.append(deploy_entry)

        edges = []
        # Check if there are already metric anomalies for this service
        # (deploy came after metrics — unusual but possible in reordered streams)
        for metric_entry in self._recent_metrics:
            if metric_entry['canonical_id'] == canonical_id:
                if self._within_window(deploy_entry['ts'], metric_entry['ts'],
                                       self.DEPLOY_ATTRIBUTION_WINDOW_MINUTES):
                    edge = self._create_edge(
                        cause_event_id=event_id,
                        effect_event_id=metric_entry['event_id'],
                        cause_cid=canonical_id,
                        effect_cid=metric_entry['canonical_id'],
                        edge_type="deploy_caused_degradation",
                        confidence=self._compute_deploy_confidence(canonical_id, deploy_entry['ts']),
                        evidence={
                            'rule': 'deploy_attribution',
                            'deploy_version': deploy_entry['version'],
                            'metric_name': metric_entry.get('metric_name', ''),
                            'metric_value': metric_entry.get('metric_value', 0),
                        },
                        ts=deploy_entry['ts'],
                    )
                    if edge:
                        edges.append(edge)
        return edges

    def _handle_metric(self, event_id: str, event: dict, canonical_id: str) -> list[dict]:
        """
        Rule 1 (part 2): Check if metric anomaly follows a recent deploy.
        Rule 4: Check for co-movement with other services.
        """
        metric_name = event.get("name", "")
        metric_value = event.get("value", 0)
        ts = event['ts']

        metric_entry = {
            'event_id': event_id,
            'canonical_id': canonical_id,
            'ts': ts,
            'metric_name': metric_name,
            'metric_value': metric_value,
            'event': event,
        }
        self._recent_metrics.append(metric_entry)

        edges = []

        # Check for anomaly relative to baseline
        is_anomalous, z_score = self.storage.is_metric_anomalous(
            canonical_id, metric_name, metric_value
        )

        # Update baseline (always, even if anomalous — Welford's handles it)
        self.storage.upsert_metric_baseline(canonical_id, metric_name, metric_value, ts)

        if not is_anomalous and not is_degradation_metric(metric_name):
            return edges

        # Rule 1: Deploy → metric degradation
        for deploy in self._recent_deploys:
            if deploy['canonical_id'] == canonical_id:
                if self._within_window(deploy['ts'], ts, self.DEPLOY_ATTRIBUTION_WINDOW_MINUTES):
                    # Deploy preceded this metric — causal
                    if deploy['ts'] <= ts:
                        edge = self._create_edge(
                            cause_event_id=deploy['event_id'],
                            effect_event_id=event_id,
                            cause_cid=canonical_id,
                            effect_cid=canonical_id,
                            edge_type="deploy_caused_degradation",
                            confidence=self._compute_deploy_confidence(canonical_id, deploy['ts']),
                            evidence={
                                'rule': 'deploy_attribution',
                                'deploy_version': deploy.get('version', ''),
                                'metric_name': metric_name,
                                'metric_value': metric_value,
                                'is_anomalous': is_anomalous,
                                'z_score': round(z_score, 2),
                            },
                            ts=ts,
                        )
                        if edge:
                            edges.append(edge)

        # Rule 4: Metric co-movement — look for other services with anomalies
        if is_anomalous:
            for other_metric in self._recent_metrics:
                if (other_metric['canonical_id'] != canonical_id and
                    other_metric.get('metric_name') == metric_name):
                    if self._within_window(other_metric['ts'], ts,
                                           self.METRIC_COMOVEMENT_WINDOW_MINUTES):
                        # Check if the other metric was also anomalous
                        other_anomalous, _ = self.storage.is_metric_anomalous(
                            other_metric['canonical_id'],
                            metric_name,
                            other_metric.get('metric_value', 0),
                        )
                        if other_anomalous:
                            edge = self._create_edge(
                                cause_event_id=other_metric['event_id'],
                                effect_event_id=event_id,
                                cause_cid=other_metric['canonical_id'],
                                effect_cid=canonical_id,
                                edge_type="metric_comovement",
                                confidence=0.3,  # Low initial — reinforced if repeats
                                evidence={
                                    'rule': 'metric_comovement',
                                    'metric_name': metric_name,
                                    'services': [
                                        self.identity.get_current_name(other_metric['canonical_id']),
                                        self.identity.get_current_name(canonical_id),
                                    ],
                                },
                                ts=ts,
                            )
                            if edge:
                                edges.append(edge)

        return edges

    def _handle_log(self, event_id: str, event: dict, canonical_id: str) -> list[dict]:
        """
        Rule 3: Error log within 2 minutes of a metric anomaly → causal edge.
        Also connects to deploy events if a deploy preceded the error.
        """
        level = event.get("level", "").lower()
        ts = event['ts']

        log_entry = {
            'event_id': event_id,
            'canonical_id': canonical_id,
            'ts': ts,
            'level': level,
            'event': event,
        }
        self._recent_logs.append(log_entry)

        edges = []

        if level not in ("error", "fatal", "critical", "warn"):
            return edges

        # Rule 3: Error log → recent metric anomaly
        for metric_entry in self._recent_metrics:
            if metric_entry['canonical_id'] == canonical_id:
                if self._within_window(metric_entry['ts'], ts,
                                       self.LOG_METRIC_CORRELATION_WINDOW_MINUTES):
                    edge = self._create_edge(
                        cause_event_id=metric_entry['event_id'],
                        effect_event_id=event_id,
                        cause_cid=canonical_id,
                        effect_cid=canonical_id,
                        edge_type="metric_preceded_error",
                        confidence=0.6,
                        evidence={
                            'rule': 'log_metric_correlation',
                            'metric_name': metric_entry.get('metric_name', ''),
                            'log_level': level,
                            'log_msg': event.get('msg', '')[:200],
                        },
                        ts=ts,
                    )
                    if edge:
                        edges.append(edge)

        # Also check if a deploy preceded this error
        for deploy in self._recent_deploys:
            if deploy['canonical_id'] == canonical_id and deploy['ts'] <= ts:
                if self._within_window(deploy['ts'], ts,
                                       self.DEPLOY_ATTRIBUTION_WINDOW_MINUTES):
                    edge = self._create_edge(
                        cause_event_id=deploy['event_id'],
                        effect_event_id=event_id,
                        cause_cid=canonical_id,
                        effect_cid=canonical_id,
                        edge_type="deploy_caused_error",
                        confidence=0.55,
                        evidence={
                            'rule': 'deploy_error_attribution',
                            'deploy_version': deploy.get('version', ''),
                            'log_level': level,
                            'log_msg': event.get('msg', '')[:200],
                        },
                        ts=ts,
                    )
                    if edge:
                        edges.append(edge)

        # Check if the log mentions another service (upstream error)
        msg = event.get('msg', '')
        for name, cid in self.identity._name_to_canonical.items():
            if name in msg and cid != canonical_id:
                # This log mentions another service → potential upstream dependency
                edge = self._create_edge(
                    cause_event_id=event_id,  # The error mentions the other service
                    effect_event_id=event_id,
                    cause_cid=cid,            # The mentioned (causal) service
                    effect_cid=canonical_id,  # The service experiencing the error
                    edge_type="upstream_error_mention",
                    confidence=0.5,
                    evidence={
                        'rule': 'log_service_mention',
                        'mentioned_service': name,
                        'log_msg': msg[:200],
                    },
                    ts=ts,
                )
                if edge:
                    edges.append(edge)
                break  # Only one mention edge per log

        return edges

    def _handle_trace(self, event_id: str, event: dict, canonical_id: str) -> list[dict]:
        """
        Rule 2: Trace spans reveal caller→callee relationships.
        High confidence — traces are ground truth for dependencies.
        """
        spans = event.get("spans", [])
        if len(spans) < 2:
            return []

        edges = []
        ts = event['ts']

        # Process spans: each consecutive pair is a caller→callee relationship
        for i in range(len(spans) - 1):
            caller_svc = spans[i].get("svc", "")
            callee_svc = spans[i + 1].get("svc", "")
            if not caller_svc or not callee_svc:
                continue

            caller_cid = self.identity.resolve(caller_svc, ts)
            callee_cid = self.identity.resolve(callee_svc, ts)

            caller_dur = spans[i].get("dur_ms", 0)
            callee_dur = spans[i + 1].get("dur_ms", 0)

            # Track dependency graph for behavioral roles
            if caller_cid not in self._trace_dependencies:
                self._trace_dependencies[caller_cid] = set()
            self._trace_dependencies[caller_cid].add(callee_cid)

            # Infer behavioral roles
            self.identity.set_behavioral_role(caller_cid, "caller")
            self.identity.set_behavioral_role(callee_cid, "callee")

            # If callee is slow (takes >80% of caller's time), strong causal signal
            if caller_dur > 0 and callee_dur > 0:
                time_ratio = callee_dur / caller_dur
                confidence = min(0.5 + time_ratio * 0.4, 0.95)

                edge = self._create_edge(
                    cause_event_id=event_id,
                    effect_event_id=event_id,  # Same trace event, different spans
                    cause_cid=callee_cid,       # The slow service caused the issue
                    effect_cid=caller_cid,      # The caller experienced the latency
                    edge_type="trace_latency_propagation",
                    confidence=confidence,
                    evidence={
                        'rule': 'trace_linkage',
                        'caller': caller_svc,
                        'callee': callee_svc,
                        'caller_dur_ms': caller_dur,
                        'callee_dur_ms': callee_dur,
                        'time_ratio': round(time_ratio, 3),
                        'trace_id': event.get('trace_id', ''),
                    },
                    ts=ts,
                )
                if edge:
                    edges.append(edge)

            # Also check if any recent deploys for the callee are causal
            for deploy in self._recent_deploys:
                if deploy['canonical_id'] == callee_cid and deploy['ts'] <= ts:
                    if self._within_window(deploy['ts'], ts,
                                           self.DEPLOY_ATTRIBUTION_WINDOW_MINUTES):
                        edge = self._create_edge(
                            cause_event_id=deploy['event_id'],
                            effect_event_id=event_id,
                            cause_cid=callee_cid,
                            effect_cid=caller_cid,
                            edge_type="deploy_caused_trace_latency",
                            confidence=0.7,
                            evidence={
                                'rule': 'trace_deploy_attribution',
                                'deploy_version': deploy.get('version', ''),
                                'callee_dur_ms': callee_dur,
                                'trace_id': event.get('trace_id', ''),
                            },
                            ts=ts,
                        )
                        if edge:
                            edges.append(edge)

        return edges

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _create_edge(
        self,
        cause_event_id: str,
        effect_event_id: str,
        cause_cid: str,
        effect_cid: str,
        edge_type: str,
        confidence: float,
        evidence: dict,
        ts: str,
    ) -> Optional[dict]:
        """Create and persist a causal edge. Returns None if duplicate."""
        # Deduplicate by (cause_event_id, effect_event_id, edge_type)
        dedup_key = (cause_event_id, effect_event_id, edge_type)
        if dedup_key in self._edge_pairs:
            return None
        self._edge_pairs.add(dedup_key)

        edge_id = str(uuid.uuid4())[:12]
        self.storage.insert_causal_edge(
            edge_id=edge_id,
            cause_event_id=cause_event_id,
            effect_event_id=effect_event_id,
            cause_canonical_id=cause_cid,
            effect_canonical_id=effect_cid,
            edge_type=edge_type,
            confidence=confidence,
            evidence=evidence,
            created_at=ts,
        )
        return {
            'edge_id': edge_id,
            'cause_event_id': cause_event_id,
            'effect_event_id': effect_event_id,
            'cause_canonical_id': cause_cid,
            'effect_canonical_id': effect_cid,
            'edge_type': edge_type,
            'confidence': confidence,
            'evidence': evidence,
        }

    def _within_window(self, ts1: str, ts2: str, window_minutes: int) -> bool:
        """
        Check if two ISO timestamps are within a time window.
        Uses string comparison as a fast heuristic for ISO 8601 timestamps.
        For more precision, parse with datetime — but string compare works for same-day.
        """
        try:
            from datetime import datetime, timedelta
            # Parse ISO 8601
            t1 = datetime.fromisoformat(ts1.replace('Z', '+00:00'))
            t2 = datetime.fromisoformat(ts2.replace('Z', '+00:00'))
            delta = abs((t2 - t1).total_seconds())
            return delta <= window_minutes * 60
        except (ValueError, TypeError):
            # Fallback: simple string proximity (won't be accurate but won't crash)
            return True

    def _compute_deploy_confidence(self, canonical_id: str, deploy_ts: str) -> float:
        """
        Compute confidence for a deploy attribution edge.
        Diluted if multiple deploys happened simultaneously.
        """
        concurrent_deploys = sum(
            1 for d in self._recent_deploys
            if self._within_window(d['ts'], deploy_ts, 2)
        )
        if concurrent_deploys <= 1:
            return 0.7
        elif concurrent_deploys <= 3:
            return 0.5
        else:
            return 0.3

    def get_trace_dependencies(self) -> dict[str, set[str]]:
        """Get the discovered dependency graph from trace analysis."""
        return dict(self._trace_dependencies)

    def reinforce_edges_for_incident(self, incident_id: str, boost: float = 0.1) -> int:
        """
        Reinforce all causal edges involved in a resolved incident.
        Called after a successful remediation.
        Returns count of reinforced edges.
        """
        episode = self.storage.get_incident_episode(incident_id)
        if not episode or not episode.get('causal_chain_ids'):
            return 0

        count = 0
        for edge_id in episode['causal_chain_ids']:
            self.storage.reinforce_edge(edge_id, boost)
            count += 1
        return count

    def decay_all_edges(self, decay: float = 0.02) -> int:
        """
        Apply decay to all causal edges. Called periodically.
        Returns count of decayed edges.
        """
        edges = self.storage.query_causal_edges(min_confidence=0.0, limit=10000)
        count = 0
        for edge in edges:
            if edge['confidence'] > 0.05:
                self.storage.decay_edge(edge['edge_id'], decay)
                count += 1
        return count
