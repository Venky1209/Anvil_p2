#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BENCH_DIR="${ANVIL_BENCH_DIR:-}"

if [[ -z "$BENCH_DIR" ]]; then
  if [[ -d "$ROOT/../Anvil-P-E/bench-p02-context" ]]; then
    BENCH_DIR="$ROOT/../Anvil-P-E/bench-p02-context"
  elif [[ -d "$ROOT/../Anvil-P-E-eval/bench-p02-context" ]]; then
    BENCH_DIR="$ROOT/../Anvil-P-E-eval/bench-p02-context"
  else
    echo "Set ANVIL_BENCH_DIR to the public bench-p02-context directory." >&2
    exit 1
  fi
fi

export PYTHONPATH="$(cd "$ROOT/.." && pwd)${PYTHONPATH:+:$PYTHONPATH}"
cd "$BENCH_DIR"

python run.py \
  --adapter Anvil.adapters.myteam:Engine \
  --mode fast \
  --seeds 314159 271828 161803 141421 173205 \
  --out report.json
