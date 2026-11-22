"""Compatibility entry point; use domeggook_API.workflows.sync_cli for new commands."""
from .sync_cli import main


if __name__ == "__main__":
    raise SystemExit(main())
