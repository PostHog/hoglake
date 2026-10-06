"""Check that readiness came from the server started by this test run."""

import json
import sys


def main() -> int:
    try:
        info = json.load(sys.stdin)
    except (ValueError, OSError):
        return 1
    return 0 if isinstance(info, dict) and info.get("name") == sys.argv[1] else 1


if __name__ == "__main__":
    sys.exit(main())
