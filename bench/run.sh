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

export PYTHONPATH="$ROOT:$BENCH_DIR${PYTHONPATH:+:$PYTHONPATH}"
cd "$ROOT"

if [[ "$#" -eq 0 ]]; then
  set -- --mode fast --out "$ROOT/report.json"
fi

python -m run \
  --adapter adapters.myteam:Engine \
  "$@"
