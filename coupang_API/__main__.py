"""Daily best is the current collector; keyword search is explicitly legacy."""
import argparse


def main(argv=None):
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--mode', choices=('best', 'legacy-search'), default='best')
    args, remaining = parser.parse_known_args(argv)
    if args.mode == 'legacy-search':
        from .workflows.collector import main as run
    else:
        from .workflows.best import main as run
    return run(remaining)


if __name__ == "__main__":
    raise SystemExit(main())
