"""Compatibility import for product_sync; existing callers share the same module."""
import sys

from . import product_sync

sys.modules[__name__] = product_sync
