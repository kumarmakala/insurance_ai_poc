from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path


STEP_PLAN = [
    ("parse",    "Parse workbook",          0.45, "Detected 9 sheets · 234 markets · 60 relationships"),
    ("classify", "Classify source",         0.30, "Schema matched: Iris v4 Entity/Human/Contact pack"),
    ("map",      "Map to canonical",        1.10, "Mapped 48 columns · 3 enum violations quarantined"),
    ("dedupe",   "Dedupe + cluster",        1.40, "Found 3 clusters · 2 auto-merged · 1 routed to HITL"),
    ("graph",    "Extract relationships",   0.70, "Built graph · 50 nodes · 58 edges · 2 new unknowns"),
    ("validate", "Validate business rules", 0.65, "All FEIN/SSN formats valid · 0 hard failures"),
    ("load",     "Load to Iris",            1.10, "Committed 17 entities · 33 humans · 71 contacts"),
]


async def dag_event_stream(xlsx_path: str | Path):
    """Yield Server-Sent Events frames for a fake but realistic Airflow-style run."""
    start = time.time()
    yield _sse("start", {"xlsx": str(xlsx_path), "ts": time.time()})
    total_in = 0
    for step_id, title, seconds, note in STEP_PLAN:
        yield _sse("step", {"id": step_id, "title": title, "status": "running"})
        await asyncio.sleep(seconds)
        records_in = total_in or 425
        records_out = records_in - (2 if step_id == "dedupe" else 0)
        total_in = records_out
        yield _sse(
            "step",
            {
                "id": step_id,
                "title": title,
                "status": "success",
                "duration_ms": int(seconds * 1000),
                "records_in": records_in,
                "records_out": records_out,
                "note": note,
            },
        )
    yield _sse("end", {"wall_ms": int((time.time() - start) * 1000)})


def _sse(event: str, data) -> bytes:
    return f"event: {event}\ndata: {json.dumps(data)}\n\n".encode("utf-8")
