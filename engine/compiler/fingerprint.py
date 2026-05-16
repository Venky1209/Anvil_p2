import math
from datetime import datetime, timedelta

def _ts(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))

def pad_to_48(vec: list[float]) -> list[float]:
    return (vec + [0.5] * 48)[:48]

def l2_normalize(vec: list[float]) -> list[float]:
    norm = math.sqrt(sum(v * v for v in vec))
    if norm > 1e-10:
        return [v / norm for v in vec]
    return vec

def recency_multiplier(past_ts: str, current_ts: str) -> float:
    try:
        dt = (_ts(current_ts) - _ts(past_ts)).total_seconds() / 3600.0
        return max(0.5, 1.0 - (dt / (24 * 30)))  # Decay 50% over 30 days
    except Exception:
        return 1.0

def compute_dimension_weights(all_vecs: list[list[float]], all_fams: list[str]) -> list[float]:
    """Fisher Discriminant Ratio (FDR) weighting."""
    if not all_vecs or not all_fams:
        return [1.0] * 48
    
    n_dims = len(all_vecs[0])
    fam_to_vecs = {}
    for vec, fam in zip(all_vecs, all_fams):
        if fam == "unknown" or not fam:
            continue
        fam_to_vecs.setdefault(fam, []).append(vec)

    if len(fam_to_vecs) < 2:
        return [1.0] * n_dims

    # Compute global mean
    global_mean = [0.0] * n_dims
    total = 0
    for vecs in fam_to_vecs.values():
        for vec in vecs:
            for i in range(n_dims):
                global_mean[i] += vec[i]
            total += 1
    if total == 0:
        return [1.0] * n_dims
    global_mean = [g / total for g in global_mean]

    fdr = [0.0] * n_dims
    for d in range(n_dims):
        between_var = 0.0
        within_var = 0.0
        for vecs in fam_to_vecs.values():
            n = len(vecs)
            if n == 0:
                continue
            f_mean = sum(v[d] for v in vecs) / n
            between_var += n * ((f_mean - global_mean[d]) ** 2)
            within_var += sum(((v[d] - f_mean) ** 2) for v in vecs)
        if within_var > 1e-9:
            fdr[d] = between_var / within_var
        else:
            fdr[d] = between_var * 100.0

    # Normalize weights
    max_fdr = max(fdr) if fdr else 0
    if max_fdr > 1e-9:
        return [0.1 + 0.9 * (v / max_fdr) for v in fdr]
    return [1.0] * n_dims

def weighted_cosine_sim(v1: list[float], v2: list[float], weights: list[float]) -> float:
    if not v1 or not v2 or len(v1) != len(v2):
        return 0.0
    dot = 0.0
    m1 = 0.0
    m2 = 0.0
    for x, y, w in zip(v1, v2, weights):
        dot += x * y * w
        m1 += (x * x) * w
        m2 += (y * y) * w
    if m1 > 0 and m2 > 0:
        return dot / math.sqrt(m1 * m2)
    return 0.0

def _compute_signal_fp(storage, signal_ts: str, pcid: str) -> list[float]:
    """Generates the 48-float topology-invariant behavioral fingerprint."""
    try:
        sts = _ts(signal_ts)
        end_ts = signal_ts
        start_ts = (sts - timedelta(minutes=60)).isoformat()
    except Exception:
        return []

    # Fetch recent events for the canonical ID
    events = storage.query_events(canonical_id=pcid, start_ts=start_ts, end_ts=end_ts, limit=5000)
    
    if not events:
        return []

    total = len(events)
    kinds = [e['kind'] for e in events]
    ts_list = []
    svcs = []
    vals = []
    
    for e in events:
        try:
            ts_list.append(_ts(e['ts']))
        except Exception:
            pass
        svcs.append(e.get('raw_service', ''))
        vals.append(e.get('metric_value', 0.0) or 0.0)
        
    def _cnt(k: str) -> int:
        return sum(1 for x in kinds if x == k)

    def _safe(fn) -> float:
        try:
            return float(fn())
        except Exception:
            return 0.5

    f = []
    
    # 1. Event Composition Ratios (0-7)
    f += [
        _safe(lambda: _cnt("deploy") / max(total, 1) * 10),
        _safe(lambda: _cnt("metric") / max(total, 1)),
        _safe(lambda: _cnt("trace") / max(total, 1)),
        _safe(lambda: _cnt("log") / max(total, 1)),
        _safe(lambda: _cnt("incident_signal") / max(total, 1) * 10),
        _safe(lambda: sum(1 for k in kinds if k not in ("deploy", "metric", "trace", "log", "incident_signal")) / max(total, 1)),
        0.5, 0.5
    ]

    # 2. Timing and Clustering (8-15)
    f += [0.5] * 8  # Simplified timing

    # 3. Structural Patterns (16-23)
    unique_svcs = len({s for s in svcs if s})
    f += [
        _safe(lambda: min(unique_svcs / 10.0, 1.0)),
        0.5, 0.5, 0.5, 0.5, 0.5,
        _safe(lambda: 1.0 if "topology" in kinds else 0.0),
        _safe(lambda: _cnt("deploy") / max(unique_svcs, 1) if unique_svcs else 0.5)
    ]

    # 4. Severity (24-31)
    metric_vals = [v for k, v in zip(kinds, vals) if k == "metric"]
    f += [
        _safe(lambda: min(max(metric_vals) / 10000.0, 1.0) if metric_vals else 0.5),
        _safe(lambda: min(_cnt("log") / max(total, 1) * 2, 1.0)),
        0.0, 0.5, 0.5, 0.5, 0.5, 0.5
    ]
    
    base_32 = (f + [0.5]*32)[:32]

    # 5. Sequence Bigrams (32-39)
    from engine.compiler.fingerprint import _compute_sequence_features
    seq_features = _compute_sequence_features(kinds)

    # 6. Remediation placeholder (40-43)
    rem_features = [0.0, 0.0, 0.0, 0.0]

    # 7. Metric Profile (44-47)
    from engine.compiler.fingerprint import _compute_metric_profile
    metric_profile = _compute_metric_profile(events)

    vec = base_32 + seq_features + rem_features + metric_profile
    return l2_normalize(pad_to_48(vec))

def _compute_sequence_features(kinds: list[str]) -> list[float]:
    counts = {
        "deploy_error": 0.0,
        "deploy_spike": 0.0,
        "error_cascade": 0.0,
        "signal_cluster": 0.0,
        "trace_error": 0.0,
    }
    for i in range(len(kinds) - 1):
        k1, k2 = kinds[i], kinds[i+1]
        if k1 == "deploy" and k2 == "log": counts["deploy_error"] += 1.0
        elif k1 == "deploy" and k2 == "metric": counts["deploy_spike"] += 1.0
        elif k1 == "log" and k2 == "log": counts["error_cascade"] += 1.0
        elif k1 == "incident_signal" and k2 == "incident_signal": counts["signal_cluster"] += 1.0
        elif k1 == "trace" and k2 == "log": counts["trace_error"] += 1.0
    
    total = max(sum(counts.values()), 1.0)
    return [
        counts["deploy_error"] / total,
        counts["deploy_spike"] / total,
        counts["error_cascade"] / total,
        counts["signal_cluster"] / total,
        counts["trace_error"] / total,
        0.5, 0.5, 0.5
    ]

def _compute_metric_profile(events: list[dict]) -> list[float]:
    mem_spikes = 0.0
    cpu_spikes = 0.0
    db_spikes = 0.0
    latency_spikes = 0.0
    
    metrics = [e for e in events if e.get('kind') == 'metric']
    for m in metrics:
        name = m.get('metric_name', '').lower()
        rj = m.get('raw_json', {})
        if not name and isinstance(rj, dict):
            name = rj.get('metric_name', rj.get('name', '')).lower()
        val = m.get('metric_value', 0.0)
        
        if val > 1000:
            if 'mem' in name or 'heap' in name: mem_spikes += 1.0
            elif 'cpu' in name or 'load' in name: cpu_spikes += 1.0
            elif 'db' in name or 'conn' in name: db_spikes += 1.0
            elif 'lat' in name or 'duration' in name: latency_spikes += 1.0
            
    total = max(mem_spikes + cpu_spikes + db_spikes + latency_spikes, 1.0)
    return [
        mem_spikes / total,
        cpu_spikes / total,
        db_spikes / total,
        latency_spikes / total
    ]
