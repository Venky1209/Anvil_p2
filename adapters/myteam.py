"""
Anvil PCE — Full Engine Adapter (v6 — Final Production).

The highest-scoring approach: uses v2's broad CID matching with
enhanced scoring that boosts incidents matching remediation targets.
"""
from __future__ import annotations

import sys
import os
from typing import Iterable, Literal
from datetime import datetime, timedelta

_engine_paths = [
    os.path.join(os.path.dirname(__file__), '..', '..', '..'),
    os.path.join(os.path.dirname(__file__), '..', '..', '..', '..'),
    '/Users/gugank/anvil/anvil-pce',
]
for p in _engine_paths:
    ap = os.path.abspath(p)
    if ap not in sys.path:
        sys.path.insert(0, ap)

from adapter import Adapter
from schema import Context, Event, IncidentSignal, CausalEdge, IncidentMatch, Remediation

from engine.ingestion.storage import DuckDBStorage
from engine.ingestion.identity import IdentityResolver
from engine.ingestion.normalizer import EventNormalizer
from engine.ingestion.event_store import EventStore
from engine.ingestion.causal import CausalEdgeDetector
from engine.ingestion.parser import parse_event


def _pts(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


class Engine(Adapter):
    def __init__(self):
        self.storage = DuckDBStorage(":memory:")
        self.identity = IdentityResolver(self.storage)
        self.normalizer = EventNormalizer(self.identity)
        self.event_store = EventStore(self.storage)
        self.causal = CausalEdgeDetector(self.storage, self.identity)
        self._active: dict[str, dict] = {}
        self._count = 0
        self._resolved: list[dict] = []

    def ingest(self, events: Iterable[Event]) -> None:
        for raw in events:
            try:
                r = dict(raw)
                if r.get('kind') == 'topology' and 'from_' in r:
                    r['from'] = r.pop('from_')
                p = parse_event(r)
                n = self.normalizer.normalize(p)
                self.event_store.append(n)
                self.causal.on_event(n['id'], p, n['canonical_service'])
                k = p.get('kind', '')
                if k == 'incident_signal':
                    self._on_signal(n, p)
                elif k == 'remediation':
                    self._on_rem(n, p)
                self._count += 1
            except Exception:
                pass

    def _on_signal(self, n, r):
        iid = r.get('incident_id', '')
        if not iid:
            return
        ts = n['ts']
        svc = r.get('service', '')
        scid = self.identity.resolve(svc, ts) if svc else ''

        cids = set()
        if scid:
            cids.add(scid)

        try:
            lb = (_pts(ts) - timedelta(minutes=60)).isoformat()
        except:
            lb = None

        for de in self.storage.query_events(kind='deploy', start_ts=lb, end_ts=ts, limit=50):
            c = de.get('canonical_id', '')
            if c:
                cids.add(c)

        if scid:
            for e in self.storage.query_causal_edges(effect_canonical_id=scid, min_confidence=0.3, limit=10):
                cids.add(e.get('cause_canonical_id', ''))
            for e in self.storage.query_causal_edges(cause_canonical_id=scid, min_confidence=0.3, limit=10):
                cids.add(e.get('effect_canonical_id', ''))

        cids.discard('')

        eids = []
        for c in cids:
            for e in self.storage.query_events(canonical_id=c, end_ts=ts, limit=50):
                eids.append(e['event_id'])
        cedges = self.storage.get_causal_edges_for_events(eids[-100:])

        ep = {
            'incident_id': iid, 'canonical_ids': list(cids), 'signal_cid': scid,
            'ts_start': ts, 'ts_incident_signal': ts, 'ts_resolved': None,
            'remediation_action': None, 'remediation_target': None,
            'remediation_target_cid': None, 'remediation_outcome': None,
            'remediation_confidence': 0.5, 'tier': 'hot', 'last_accessed_ts': ts,
            'event_ids': eids[-50:], 'causal_chain_ids': [e['edge_id'] for e in cedges],
            'fingerprint_vec': None, 'family_id': None,
        }
        self._active[iid] = ep
        self.storage.upsert_incident_episode(ep)

    def _on_rem(self, n, r):
        iid = r.get('incident_id', '')
        if not iid:
            return
        ts, act, tgt, out = n['ts'], r.get('action', ''), r.get('target', ''), r.get('outcome', '')
        tcid = self.identity.resolve(tgt, ts) if tgt else ''

        ep = self._active.get(iid, {
            'incident_id': iid, 'canonical_ids': [tcid] if tcid else [],
            'signal_cid': tcid, 'ts_start': ts, 'ts_incident_signal': ts,
            'event_ids': [], 'causal_chain_ids': [],
            'fingerprint_vec': None, 'family_id': None,
        })
        if tcid and tcid not in ep.get('canonical_ids', []):
            ep.setdefault('canonical_ids', []).append(tcid)

        ep.update({
            'ts_resolved': ts, 'remediation_action': act,
            'remediation_target': tgt, 'remediation_target_cid': tcid,
            'remediation_outcome': out,
            'remediation_confidence': 0.8 if out == 'resolved' else 0.3,
            'tier': 'warm', 'last_accessed_ts': ts,
        })
        self.storage.upsert_incident_episode(ep)
        if out == 'resolved':
            self._resolved.append(ep)
            self.causal.reinforce_edges_for_incident(iid, boost=0.1)
        self._active.pop(iid, None)

    def reconstruct_context(
        self, signal: IncidentSignal, mode: Literal["fast", "deep"] = "fast",
    ) -> Context:
        iid = signal.get("incident_id", "")
        sts = signal.get("ts", "")
        ssvc = signal.get("service", "")
        trigger = signal.get("trigger", "")

        tsvc = self._xsvc(trigger) or ssvc
        pcid = self.identity.resolve(tsvc, sts) if tsvc else ''

        rcids = set()
        if pcid:
            rcids.add(pcid)

        try:
            lb = (_pts(sts) - timedelta(minutes=60)).isoformat()
        except:
            lb = None

        for dr in self.storage.query_events(kind='deploy', start_ts=lb, end_ts=sts, limit=30):
            c = dr.get('canonical_id', '')
            if c:
                rcids.add(c)

        if pcid:
            for e in self.storage.query_causal_edges(effect_canonical_id=pcid, min_confidence=0.2, limit=10):
                rcids.add(e.get('cause_canonical_id', ''))
            for e in self.storage.query_causal_edges(cause_canonical_id=pcid, min_confidence=0.2, limit=10):
                rcids.add(e.get('effect_canonical_id', ''))

        rcids.discard('')

        # Related events
        rel = []
        lim = 30 if mode == "fast" else 100
        for c in rcids:
            for e in self.storage.query_events(canonical_id=c, end_ts=sts, limit=lim):
                raw = e.get('raw_json', {})
                if isinstance(raw, dict) and raw:
                    rel.append(raw)
        rel.sort(key=lambda e: e.get('ts', ''), reverse=True)
        rel = rel[:lim]

        # Causal chain
        chain, cseen = [], set()
        for c in rcids:
            for edge in self.storage.query_causal_edges(cause_canonical_id=c, min_confidence=0.2, limit=15):
                if edge['edge_id'] not in cseen:
                    cseen.add(edge['edge_id'])
                    chain.append(CausalEdge(
                        cause_event_id=edge['cause_event_id'],
                        effect_event_id=edge['effect_event_id'],
                        evidence=str(edge.get('evidence', '')),
                        confidence=edge['confidence'],
                    ))
            for edge in self.storage.query_causal_edges(effect_canonical_id=c, min_confidence=0.2, limit=15):
                if edge['edge_id'] not in cseen:
                    cseen.add(edge['edge_id'])
                    chain.append(CausalEdge(
                        cause_event_id=edge['cause_event_id'],
                        effect_event_id=edge['effect_event_id'],
                        evidence=str(edge.get('evidence', '')),
                        confidence=edge['confidence'],
                    ))
        chain.sort(key=lambda e: e.get('confidence', 0), reverse=True)
        chain = chain[:10]

        # Similar past incidents — broad match with smart scoring
        matches = []
        seen = set()
        all_eps = self.storage.get_all_incident_episodes()
        for ep in all_eps:
            eid = ep.get('incident_id', '')
            if eid == iid or eid in seen:
                continue
            if not ep.get('remediation_action'):
                continue

            ep_cids = set(ep.get('canonical_ids', []))
            overlap = rcids & ep_cids
            if not overlap:
                continue

            seen.add(eid)
            sim = 0.4

            # Boost: resolved successfully
            if ep.get('remediation_outcome') == 'resolved':
                sim += 0.15

            # Boost: remediation target matches primary CID (strongest signal)
            rem_tcid = ep.get('remediation_target_cid', '')
            if rem_tcid == pcid and pcid:
                sim += 0.25
            elif rem_tcid in rcids:
                sim += 0.1

            # Boost: canonical ID overlap ratio
            overlap_r = len(overlap) / max(len(rcids), 1)
            sim += overlap_r * 0.15

            # Boost: primary CID directly in episode's CIDs
            if pcid and pcid in ep_cids:
                sim += 0.1

            # Boost: has causal chain
            if ep.get('causal_chain_ids'):
                sim += 0.05

            sim = min(sim, 0.95)
            rationale = f"Resolved via {ep.get('remediation_action','?')}" if ep.get('remediation_outcome') == 'resolved' else "Same service identity"

            matches.append(IncidentMatch(
                incident_id=eid, similarity=sim, rationale=rationale,
            ))

        matches.sort(key=lambda m: m.get('similarity', 0), reverse=True)
        matches = matches[:5]

        # Remediations
        rems, rseen = [], set()
        for ep in self._resolved:
            ep_cids = set(ep.get('canonical_ids', []))
            if not (rcids & ep_cids):
                continue
            act = ep.get('remediation_action', '')
            if not act or act in rseen:
                continue
            rseen.add(act)
            tc = ep.get('remediation_target_cid', '')
            tgt = self.identity.get_current_name(tc) if tc else ep.get('remediation_target', '')
            rems.append(Remediation(action=act, target=tgt or '?',
                                    historical_outcome='resolved', confidence=0.8))
        rems = rems[:5]

        conf = min(0.1 + (0.2 if rel else 0) + (0.3 if chain else 0) + (0.3 if matches else 0), 1.0)

        # Explain
        parts = [f"Incident {iid}:"]
        if pcid:
            nm = self.identity.get_current_name(pcid)
            al = self.identity.get_all_aliases(pcid)
            if len(al) > 1:
                parts.append(f"'{nm}' (aliases: {', '.join(a for a in al if a != nm)})")
            else:
                parts.append(f"'{nm}'")
        if chain:
            parts.append(f"{len(chain)} causal edges.")
        if matches:
            parts.append(f"{len(matches)} similar incidents.")
        if rems:
            parts.append(f"Suggested: {rems[0].get('action')} on {rems[0].get('target')}")

        return Context(
            related_events=rel, causal_chain=chain,
            similar_past_incidents=matches, suggested_remediations=rems,
            confidence=conf, explain=" ".join(parts),
        )

    def _xsvc(self, trigger):
        if not trigger:
            return ''
        if ':' in trigger:
            a = trigger.split(':', 1)[1]
            return a.split('/', 1)[0] if '/' in a else a
        return ''

    def close(self):
        self.storage.close()
