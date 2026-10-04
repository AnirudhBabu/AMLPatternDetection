"""Kept for backwards compatibility: `python data_pattern_scanner.py` runs the whole pipeline.

It is the same as `uv run aml run-all`. Any `aml` arguments are passed through, e.g.
`python data_pattern_scanner.py cycles --max-window-days 21`.
"""

import sys

from aml.cli import main

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:] or ["run-all"]))
