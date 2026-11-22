"""Independent supply scoring CLI. Default is offline plan; never reads .env."""
import argparse
import json
import os

from product_data_collector.domain.product_events import utc


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=('initial','incremental'))
    parser.add_argument('--source', required=True, choices=('domeggook','ownerclan'))
    parser.add_argument('--run-id', required=True)
    parser.add_argument('--as-of', required=True, help='Fixed timezone-aware ISO timestamp; retain for resume')
    parser.add_argument('--chunk-size', type=int, default=500)
    parser.add_argument('--max-chunks', type=int)
    parser.add_argument('--execute', action='store_true')
    parser.add_argument('--dsn-env')
    args = parser.parse_args(argv)
    as_of = utc(args.as_of)
    if not 1<=args.chunk_size<=10000 or (args.max_chunks is not None and args.max_chunks<1):
        parser.error('Invalid chunk limits')
    if not args.execute:
        print(json.dumps({'mode':args.mode,'source':args.source,'runId':args.run_id,'asOf':as_of.isoformat(),
                          'chunkSize':args.chunk_size,'execute':False,'migration':'stage7_supply_scoring (separate command)'}))
        return 0
    if not args.dsn_env or not os.environ.get(args.dsn_env):
        parser.error('--execute requires explicit --dsn-env; .env is never read')
    import psycopg
    from psycopg.rows import dict_row
    from product_data_collector.persistence.scoring import run_scoring
    with psycopg.connect(os.environ[args.dsn_env], row_factory=dict_row) as conn:
        result = run_scoring(conn,run_id=args.run_id,mode=args.mode,platform=args.source,as_of=as_of,
                             chunk_size=args.chunk_size,max_chunks=args.max_chunks)
        print(json.dumps({'runId':result['run_id'],'status':result['status'],'processed':result['processed']}))
    return 0


if __name__=='__main__':
    raise SystemExit(main())
