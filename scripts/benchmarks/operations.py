"""Re-run existing optimized pipeline fixtures on a dedicated disposable server."""
import argparse
import json
from pathlib import Path
from scripts.benchmarks import pipeline as benchmark


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--execute', action='store_true')
    args = parser.parse_args()
    if not args.execute:
        print(json.dumps({'execute': False, 'size': 1000, 'external_api_calls': 0}))
        return
    benchmark.CONTAINER = 'chozy-operations-performance'
    benchmark.OUT = Path('artifacts/experiments/operations-readiness/scoring')
    original_apply = benchmark.apply_migration

    def apply(conn, name):
        result = original_apply(conn, name)
        if name == 'stage7_history_policy_v2':
            original_apply(conn, 'stage10_scoring_performance')
        return result

    benchmark.apply_migration = apply
    try:
        benchmark.run(argparse.Namespace(size=1000, seed=917, max_options=5,
                      history_changes=4, vendors=30, skew=.2, change_rate=.01))
    finally:
        benchmark.apply_migration = original_apply


if __name__ == '__main__':
    main()
