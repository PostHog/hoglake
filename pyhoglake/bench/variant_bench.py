"""Microseconds per row of variant.encode_json on PostHog-like properties.

Not part of CI or the suite: a number to quote, and to compare against when
the encoder changes (the determinism pin is tests/data/variant/
golden_storage.arrow, not this). Run it on the interpreter you deploy:

    cd pyhoglake && flox activate -- uv run python bench/variant_bench.py
    uv run --python 3.12 --isolated --with-editable . python bench/variant_bench.py

The rows imitate posthog-js ``properties``: about 1 KB of compact JSON and
26 top-level keys, with a nested ``$set`` object, a feature-flag array,
URLs, a user agent and a few numbers. One declaration shreds the 12 keys a
writer would most likely declare, an array of strings among them; the
other is millrace's properties_sv, 16 string fields. The generator is
seeded, so two runs encode the same text. ``--payload dev`` pads each row
to about 4.3 KB, mw-dev's average, with the keys a dev event adds.
PYHOGLAKE_VARIANT_CODEC picks the codec.
"""

from __future__ import annotations

import argparse
import json
import platform
import random
import statistics
import string
import sys
import time

import pyarrow as pa

from pyhoglake import _variant_backend, variant

SHREDDING_12 = {
    "type": "object",
    "fields": [
        {"name": "$browser", "type": "string"},
        {"name": "$os", "type": "string"},
        {"name": "$device_type", "type": "string"},
        {"name": "$current_url", "type": "string"},
        {"name": "$pathname", "type": "string"},
        {"name": "$lib", "type": "string"},
        {"name": "$lib_version", "type": "string"},
        {"name": "$session_id", "type": "string"},
        {"name": "$referrer", "type": "string"},
        {"name": "$screen_width", "type": "int64"},
        {"name": "$geoip_latitude", "type": "double"},
        {
            "name": "$active_feature_flags",
            "type": "array",
            "element": {"type": "string"},
        },
    ],
}

#: millrace's properties_sv declaration on ingest.events.
MILLRACE_16 = {
    "type": "object",
    "fields": [
        {"name": name, "type": "string"}
        for name in ("$session_id", "$window_id", *(f"$group_{i}" for i in range(5)),
                     "$current_url", "$pathname", "$host", "$browser", "$os",
                     "$device_type", "$lib", "$referring_domain", "$device_id")
    ],
}  # fmt: skip

_PATHS = ["/dashboard", "/insights/new", "/events", "/persons", "/feature_flags",
          "/sessions", "/settings", "/web-analytics", "/experiments", "/surveys"]  # fmt: skip
_FLAGS = ["new-dashboard-layout", "session-replay-v2", "billing-redesign",
          "query-performance", "data-pipelines", "hog-ql", "web-analytics",
          "notebook-mode", "error-tracking", "surveys-v2"]  # fmt: skip
_AGENT = (
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/{v}.0.0.0 Safari/537.36"
)


def _id(rng: random.Random, n: int) -> str:
    return "".join(rng.choices(string.hexdigits.lower(), k=n))


def properties(rng: random.Random, i: int) -> str:
    path = rng.choice(_PATHS)
    url = f"https://app.posthog.com/project/{rng.randrange(1, 5000)}{path}"
    props = {
        "$browser": rng.choice(["Chrome", "Firefox", "Safari", "Edge"]),
        "$browser_version": rng.randrange(95, 126),
        "$os": rng.choice(["Mac OS X", "Windows", "Linux", "iOS", "Android"]),
        "$os_version": f"{rng.randrange(10, 15)}.{rng.randrange(0, 8)}",
        "$device_type": rng.choice(["Desktop", "Mobile", "Tablet"]),
        "$current_url": url + ("?ref=" + _id(rng, 8) if rng.random() < 0.3 else ""),
        "$host": "app.posthog.com",
        "$pathname": path,
        "$raw_user_agent": _AGENT.format(v=rng.randrange(95, 126)),
        "$screen_width": rng.choice([1280, 1440, 1920, 2560, 390]),
        "$screen_height": rng.choice([720, 900, 1080, 1440, 844]),
        "$viewport_width": rng.randrange(300, 2560),
        "$lib": "web",
        "$lib_version": f"1.{rng.randrange(100, 200)}.{rng.randrange(0, 10)}",
        "$insert_id": _id(rng, 16),
        "$time": 1.7598e9 + i / 1000 + rng.random(),
        "$session_id": f"0192{_id(rng, 4)}-{_id(rng, 4)}-7{_id(rng, 3)}-{_id(rng, 12)}",
        "$window_id": f"0192{_id(rng, 4)}-{_id(rng, 4)}-7{_id(rng, 3)}-{_id(rng, 12)}",
        "$device_id": f"0192{_id(rng, 12)}-{_id(rng, 4)}",
        "$referrer": rng.choice(["https://www.google.com/", "$direct", ""]),
        "$geoip_latitude": round(rng.uniform(-60, 60), 4),
        "$geoip_country_code": rng.choice(["US", "GB", "DE", "FR", "JP", "BR"]),
        "$active_feature_flags": rng.sample(_FLAGS, rng.randrange(0, 6)),
        "$is_identified": rng.random() < 0.4,
        "token": "phc_" + _id(rng, 24),
        "$set": {
            "$browser": "Chrome",
            "$current_url": url,
            "email": f"user{rng.randrange(10**6)}@example.com",
        },
    }
    return json.dumps(props, separators=(",", ":"))


def dev_properties(rng: random.Random, i: int) -> str:
    props = json.loads(properties(rng, i))
    props["$set_once"] = {
        f"$initial_{k[1:]}": v for k, v in props.items() if k[0] == "$"
    }
    props["$feature_flag_payloads"] = {f: {"variant": _id(rng, 6)} for f in _FLAGS}
    props["$elements_chain"] = ";".join(
        f'div.c-{_id(rng, 6)}:nth-child="{n}"attr__class="c-{_id(rng, 8)}"'
        for n in range(24)
    )
    props.update({f"$feature/{f}": rng.random() < 0.5 for f in _FLAGS})
    return json.dumps(props, separators=(",", ":"))


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n", 1)[0])
    parser.add_argument("--rows", type=int, default=50_000)
    parser.add_argument("--repeat", type=int, default=5)
    parser.add_argument("--payload", choices=["bench", "dev"], default="bench")
    args = parser.parse_args()

    rng = random.Random(20261008)
    make = dev_properties if args.payload == "dev" else properties
    rows = [make(rng, i) for i in range(args.rows)]
    text = pa.array(rows, pa.json_())
    size = sum(map(len, rows)) / len(rows)
    keys = statistics.mean(len(json.loads(r)) for r in rows[:2000])

    def run(shredding: object) -> float:
        best = float("inf")
        for _ in range(args.repeat):
            started = time.process_time()
            variant.encode_json(text, shredding=shredding)
            best = min(best, time.process_time() - started)
        return best / len(rows) * 1e6

    variant.encode_json(text.slice(0, 1000))  # warm up
    unshredded = run(None)
    print(
        f"python {platform.python_implementation()} {sys.version.split()[0]}, "
        f"pyarrow {pa.__version__}, {platform.machine()}, codec {_variant_backend.MODE}"
    )
    print(f"{len(rows)} rows, {size:.0f} B and {keys:.1f} top-level keys a row")
    print(
        f"encode_json, unshredded:           {unshredded:.2f} us/row "
        f"({unshredded * 1000 / size:.2f} us/KB), CPU time, best of {args.repeat}"
    )
    for name, shredding in (("12-field", SHREDDING_12), ("millrace", MILLRACE_16)):
        shredded = run(shredding)
        print(
            f"encode_json, {name + ' declaration:':21} {shredded:.2f} us/row "
            f"({shredded * 1000 / size:.2f} us/KB)"
        )


if __name__ == "__main__":
    main()
