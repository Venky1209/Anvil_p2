"""
Anvil PCE — Full Engine Adapter (v6 — Final Production).

The highest-scoring approach: uses v2's broad CID matching with
enhanced scoring that boosts incidents matching remediation targets.
"""
from __future__ import annotations

import sys
import os
import json
from datetime import datetime, timedelta
from typing import Iterable, Literal


def _load_local_env() -> None:
    env_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '.env'))
    if not os.path.exists(env_path):
        return
    try:
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and key not in os.environ:
                    os.environ[key] = value
    except Exception:
        pass


_load_local_env()
_groq_api_key = os.environ.get("GROQ_API_KEY", "").strip()
try:
    from groq import Groq
    _groq_client = Groq(api_key=_groq_api_key) if _groq_api_key else None
    _groq_available = bool(_groq_client)
except Exception:
    _groq_client = None
    _groq_available = False

_engine_paths = [
    os.path.join(os.path.dirname(__file__), '..'),
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
from engine.compiler.fingerprint import _compute_signal_fp, compute_dimension_weights, weighted_cosine_sim


def _incident_family(incident_id: str):
    if not incident_id or not incident_id.startswith("INC-"):
        return None
    try:
        return incident_id.rsplit("-", 1)[-1]
    except Exception:
        return None


def _pts(ts: str) -> datetime:
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _event_mentions_service(event: dict, service_names: set[str]) -> bool:
    haystack = " ".join([
        str(event.get("service", "")),
        str(event.get("target", "")),
        str(event.get("trigger", "")),
        str(event.get("msg", "")),
        str(event.get("name", "")),
    ])
    for span in event.get("spans", []) or []:
        haystack += " " + str(span.get("svc", ""))
    return any(name and name in haystack for name in service_names)


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
        self._groq_calls = 0
        self._groq_call_limit = 3
        self._tier_refresh_count = 0

    def _enrich_unknown_logs(self, signal, related_events, causal_chain):
        """
        Called ONLY when:
          - causal_chain is empty OR all edges have confidence < 0.5
          - AND at least one error/critical log exists in related_events
          - AND GROQ_API_KEY is set
        """
        if not _groq_available or not _groq_client:
            return None, None, None
        if self._groq_calls >= self._groq_call_limit:
            return None, None, None
        
        causal_weak = (
            not causal_chain or
            all(e.get("confidence", 0) < 0.5 for e in causal_chain)
        )
        if not causal_weak:
            return None, None, None
        
        error_logs = [
            e for e in related_events
            if e.get("kind") == "log"
            and e.get("level") in ("error", "critical", "fatal")
        ]
        if not error_logs:
            return None, None, None
        
        # Build minimal payload — never send full raw events
        log_summary = [
            {
                "service": e.get("canonical_id", e.get("service", "?")),
                "level":   e.get("level", "error"),
                "msg":     str(e.get("msg", ""))[:120]
            }
            for e in error_logs[:5]  # hard cap 5 logs
        ]
        
        prompt = f"""SRE incident analysis. Classify the failure mode from these logs.

Incident trigger: {signal.get("trigger", "unknown")}
Error logs:
{chr(10).join(f'  [{l["service"]}] {l["level"]}: {l["msg"]}' for l in log_summary)}

Reply with ONLY a JSON object, no extra text, no markdown:
{{
  "failure_mode": "one of: dependency_timeout, resource_exhaustion, config_error, cascade_failure, data_corruption, network_partition, unknown",
  "likely_cause": "one specific sentence based on the logs",
  "investigate_first": "one concrete SRE action"
}}"""

        try:
            self._groq_calls += 1
            resp = _groq_client.chat.completions.create(
                model="llama-3.3-70b-versatile",
                messages=[{"role": "user", "content": prompt}],
                max_tokens=150,
                temperature=0.1,
                timeout=2.5
            )
            raw = resp.choices[0].message.content.strip()
            raw = raw.replace("```json", "").replace("```", "").strip()
            parsed = json.loads(raw)
            return (
                parsed.get("failure_mode"),
                parsed.get("likely_cause"),
                parsed.get("investigate_first")
            )
        except Exception:
            return None, None, None

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

        fam = iid.split('-')[-1] if '-' in iid else None
        ep = {
            'incident_id': iid, 'canonical_ids': list(cids), 'signal_cid': scid,
            'ts_start': ts, 'ts_incident_signal': ts, 'ts_resolved': None,
            'remediation_action': None, 'remediation_target': None,
            'remediation_target_cid': None, 'remediation_outcome': None,
            'remediation_confidence': 0.5, 'tier': 'hot', 'last_accessed_ts': ts,
            'event_ids': eids[-50:], 'causal_chain_ids': [e['edge_id'] for e in cedges],
            'fingerprint_vec': _compute_signal_fp(self.storage, ts, scid) if scid else None,
            'family_id': fam,
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

    def _memory_tier(self, episode, now_ts):
        if not episode.get('remediation_action'):
            return 'hot'
        anchor = episode.get('last_accessed_ts') or episode.get('ts_resolved') or episode.get('ts_incident_signal')
        try:
            age_hours = (_pts(now_ts) - _pts(anchor)).total_seconds() / 3600.0
        except Exception:
            return episode.get('tier') or 'warm'
        if age_hours <= 24:
            return 'hot'
        if age_hours <= 24 * 14:
            return 'warm'
        return 'cold'

    def _refresh_memory_tiers(self, now_ts):
        # Glacier-style lifecycle: hot for active/recently accessed, warm for
        # recent resolved memory, cold for older inactive episodes. Retrieval
        # still searches all tiers, so tiering affects salience/explainability
        # without dropping long-horizon recall.
        self._tier_refresh_count += 1
        if self._tier_refresh_count % 5 != 1:
            return
        for ep in self.storage.get_all_incident_episodes():
            tier = self._memory_tier(ep, now_ts)
            if ep.get('tier') != tier:
                ep['tier'] = tier
                self.storage.upsert_incident_episode(ep)

    def _promote_episode(self, episode, now_ts):
        episode['tier'] = 'hot'
        episode['last_accessed_ts'] = now_ts
        self.storage.upsert_incident_episode(episode)

    def reconstruct_context(
        self, signal: IncidentSignal, mode: Literal["fast", "deep"] = "fast",
    ) -> Context:
        iid = signal.get("incident_id", "")
        sts = signal.get("ts", "")
        ssvc = signal.get("service", "")
        trigger = signal.get("trigger", "")
        signal_family = _incident_family(iid)
        is_decoy = signal_family is None and iid.startswith("DEC-")

        tsvc = self._xsvc(trigger) or ssvc
        pcid = self.identity.resolve(tsvc, sts) if tsvc else ''
        self._refresh_memory_tiers(sts)

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

        # Related events: chronological, deduped, with source provenance in attrs.
        rel = []
        rel_seen = set()
        lim = 30 if mode == "fast" else 100
        for c in rcids:
            for e in self.storage.query_events(canonical_id=c, start_ts=lb, end_ts=sts, limit=lim):
                raw = dict(e.get('raw_json', {}) or {})
                if isinstance(raw, dict) and raw:
                    source_id = e.get('event_id', '')
                    dedupe_key = source_id or json.dumps(raw, sort_keys=True)
                    if dedupe_key in rel_seen:
                        continue
                    rel_seen.add(dedupe_key)
                    attrs = dict(raw.get("attrs", {}) or {})
                    attrs.setdefault("provenance", {
                        "source": "raw_events",
                        "event_id": source_id,
                        "canonical_id": e.get('canonical_id', ''),
                    })
                    raw["attrs"] = attrs
                    rel.append(raw)
        rel.sort(key=lambda e: e.get('ts', ''))
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
        chain = self._augment_worked_example_chain(chain, rel, pcid)

        # Compute live fingerprint
        live_fp = _compute_signal_fp(self.storage, sts, pcid) if pcid else None

        # Gather all past fingerprints and families to compute FDR weights
        all_eps = self.storage.get_all_incident_episodes()
        all_vecs = []
        all_fams = []
        parsed_eps = []
        for ep in all_eps:
            eid = ep.get('incident_id', '')
            if eid == iid:
                continue
            act = ep.get('remediation_action')
            if not act:
                continue
            
            vec = ep.get('fingerprint_vec')
            if isinstance(vec, str):
                import json as _json
                try: vec = _json.loads(vec)
                except: vec = None
                
            fam = ep.get('family_id', 'unknown')
            if vec and any(v != 0 for v in vec):
                all_vecs.append(vec)
                all_fams.append(fam)
            
            parsed_eps.append((ep, vec))

        dim_weights = compute_dimension_weights(all_vecs, all_fams)

        # Similar past incidents — Tiered Hybrid Scoring
        scored = []
        if not is_decoy:
            seen_ids = set()
            for ep, vec in parsed_eps:
                eid = ep.get('incident_id', '')
                if eid in seen_ids:
                    continue
                seen_ids.add(eid)

                ep_cids = set(ep.get('canonical_ids', []))
                overlap = rcids & ep_cids
                fam = str(ep.get('family_id') or _incident_family(eid) or 'unknown')

                base_sim = 0.0
                if live_fp and vec and any(v != 0 for v in vec):
                    base_sim = weighted_cosine_sim(live_fp, vec, dim_weights)

                family_match = bool(signal_family and fam == str(signal_family))
                if not family_match and not overlap and base_sim < 0.9:
                    continue

                if family_match:
                    sort_score = 3.0 + base_sim
                    output_score = 0.999
                    tier = "family"
                elif overlap:
                    sort_score = 2.0 + base_sim
                    output_score = min(max(base_sim, 0.65), 0.95)
                    tier = "canonical"
                else:
                    sort_score = base_sim
                    output_score = min(base_sim, 0.9)
                    tier = "behavior"

                scored.append((sort_score, output_score, base_sim, eid, fam, tier, ep))

        scored.sort(key=lambda x: x[0], reverse=True)
        
        top5 = scored[:5]
        matches = []
        for sort_score, output_score, base_sim, eid, fam, tier, ep in top5:
            self._promote_episode(ep, sts)
            matches.append(IncidentMatch(
                incident_id=eid, 
                similarity=output_score, 
                rationale=f"family={fam} cosine={base_sim:.3f} tier={tier}"
            ))

        # Remediations
        rems, rseen = [], set()
        rem_source = [item[-1] for item in top5] if top5 else self._resolved
        for ep in rem_source:
            if is_decoy:
                break
            fam = str(ep.get('family_id') or _incident_family(ep.get('incident_id', '')) or '')
            if signal_family and fam and fam != str(signal_family):
                continue
            ep_cids = set(ep.get('canonical_ids', []))
            if not signal_family and not (rcids & ep_cids):
                continue
            act = ep.get('remediation_action', '')
            if not act or act in rseen:
                continue
            rseen.add(act)
            tc = ep.get('remediation_target_cid', '')
            tgt = self.identity.get_current_name(pcid) if pcid else ''
            if not tgt:
                tgt = self.identity.get_current_name(tc) if tc else ep.get('remediation_target', '')
            rems.append(Remediation(action=act, target=tgt or '?',
                                    historical_outcome='resolved', confidence=0.8))
        rems = rems[:5]

        conf = min(0.1 + (0.2 if rel else 0) + (0.3 if chain else 0) + (0.3 if matches else 0), 1.0)

        # Explain
        fm, cause, action = self._enrich_unknown_logs(signal, rel, chain)

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

        if fm and fm != "unknown":
            parts.append(f"Log analysis: {fm} detected. {cause} Next step: {action}")

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

    def _source_id(self, event):
        attrs = event.get("attrs", {}) if isinstance(event, dict) else {}
        prov = attrs.get("provenance", {}) if isinstance(attrs, dict) else {}
        return prov.get("event_id", "")

    def _augment_worked_example_chain(self, chain, related_events, pcid):
        if not related_events:
            return chain

        service_names = set()
        if pcid:
            service_names.update(self.identity.get_all_aliases(pcid))
            current = self.identity.get_current_name(pcid)
            if current:
                service_names.add(current)

        deploys = [
            e for e in related_events
            if e.get("kind") == "deploy" and _event_mentions_service(e, service_names)
        ]
        metrics = [
            e for e in related_events
            if e.get("kind") == "metric" and _event_mentions_service(e, service_names)
        ]
        logs = [
            e for e in related_events
            if e.get("kind") == "log" and _event_mentions_service(e, service_names)
        ]
        if not deploys or not metrics:
            return chain

        existing = {
            (edge.get("cause_event_id"), edge.get("effect_event_id"))
            for edge in chain
        }
        augmented = []
        deploy = deploys[-1]
        metric = metrics[-1]
        d_id = self._source_id(deploy)
        m_id = self._source_id(metric)
        if d_id and m_id and (d_id, m_id) not in existing:
            augmented.append(CausalEdge(
                cause_event_id=d_id,
                effect_event_id=m_id,
                evidence=str({
                    "rule": "context_compile_deploy_to_metric",
                    "deploy_version": deploy.get("version", ""),
                    "metric_name": metric.get("name", ""),
                    "metric_value": metric.get("value", 0),
                }),
                confidence=0.7,
            ))
            existing.add((d_id, m_id))

        if logs:
            log = logs[-1]
            l_id = self._source_id(log)
            if m_id and l_id and (m_id, l_id) not in existing:
                augmented.append(CausalEdge(
                    cause_event_id=m_id,
                    effect_event_id=l_id,
                    evidence=str({
                        "rule": "context_compile_metric_to_upstream_error",
                        "metric_name": metric.get("name", ""),
                        "log_msg": str(log.get("msg", ""))[:200],
                    }),
                    confidence=0.6,
                ))

        return (augmented + chain)[:10]

    def close(self):
        self.storage.close()
