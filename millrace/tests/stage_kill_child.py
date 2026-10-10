"""Child process for the hard-kill durability test
(tests/test_stage_integration.py::test_durability_across_hard_kill).

NOT a test module — pytest never collects it (no ``test_`` name, no
test functions). It stages batches in a loop against a ``file:///``
SlateDB path, printing one ``ACK <batch> <first> <last>`` line per
durable batch, until the parent SIGKILLs it mid-stream. Payloads come
from stagekit.payload_for so the parent can re-derive every byte.
"""

from __future__ import annotations

import asyncio
import sys

from stagekit import NOW, fast_flush_settings, payload_for


async def _run(
    path: str, batch_count: int, batch_size: int, payload_size: int, teams: int
) -> None:
    from millrace.stage import PartitionStage, StagedRecord

    stage = await PartitionStage.open(
        "file:///",
        path,
        topic="events",
        partition=0,
        settings=fast_flush_settings(),
    )
    print("READY", flush=True)
    offset = 0
    for batch_index in range(batch_count):
        records = [
            StagedRecord(
                team_id=(offset + i) % teams,
                event_ts_us=1_700_000_000_000_000 + offset + i,
                offset=offset + i,
                payload=payload_for(offset + i, payload_size),
            )
            for i in range(batch_size)
        ]
        ack = await stage.stage_batch(records, now_us=NOW + batch_index)
        print(f"ACK {batch_index} {ack.first_offset} {ack.last_offset}", flush=True)
        offset += batch_size
    print("DONE", flush=True)
    # Stay alive: the parent decides when the stream is "mid".
    await asyncio.Event().wait()


def main() -> None:
    path, batch_count, batch_size, payload_size, teams = (
        sys.argv[1],
        int(sys.argv[2]),
        int(sys.argv[3]),
        int(sys.argv[4]),
        int(sys.argv[5]),
    )
    asyncio.run(_run(path, batch_count, batch_size, payload_size, teams))


if __name__ == "__main__":
    main()
