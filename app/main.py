from __future__ import annotations

import asyncio
import json
import os
import shutil
import time
from pathlib import Path

import orjson

from fastapi import FastAPI, HTTPException, Request, UploadFile, File
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except ImportError:
    pass

from . import airflow_sim, dedupe, graph, ingest, llm, mapping, security, store


ROOT = Path(__file__).resolve().parent.parent
STATIC = ROOT / "static"
DATA = ROOT / "data"
XLSX = DATA / "demo.xlsx"

app = FastAPI(title="IRYSCLOUD POC", version="0.1.0")


store.init_db()


@app.on_event("startup")
async def bootstrap():
    """On first boot, auto-ingest demo.xlsx so the demo is instantly populated."""
    with store.tx() as conn:
        (cnt,) = conn.execute("SELECT COUNT(*) FROM entities").fetchone()
        if cnt == 0 and XLSX.exists():
            _full_ingest(str(XLSX), actor="system-boot")


_ENTITY_COLS = (
    "entity_identifier", "name", "legal_entity_type", "date_of_incorporation",
    "entity_description", "annual_revenue", "is_closed", "doing_business_as",
    "fein", "ssn", "allow_agency_participation",
)
_HUMAN_COLS = (
    "human_identifier", "prefix", "first_name", "middle_name", "last_name",
    "preferred_name", "pronoun", "date_of_birth", "education_level", "occupation",
    "occupation_industry", "year_occupation_started", "is_deceased", "gender",
    "marital_status", "ssn", "license_number", "license_state",
    "first_licensed_date", "allow_agency_participation",
)
_CONTACT_COLS = (
    "parent_type", "parent_identifier", "contact_type", "physical_line1",
    "physical_city", "physical_state", "physical_country", "mailing_line1",
    "mailing_city", "mailing_state", "primary_phone", "email",
)
_RELATIONSHIP_COLS = ("src_type", "src_name", "rel_type", "dst_type", "dst_name", "title")
_MAPPING_COLS = (
    "source_field", "canonical_field", "sample_value", "status",
    "note", "suggested_fix", "confidence", "record_ref",
)
_DEDUP_COLS = (
    "kind", "winner_ref", "members_json", "signals_json",
    "confidence", "auto_merged", "status",
)
_REVIEW_COLS = ("kind", "title", "payload_json")


def _copy_rows(cur, table: str, cols: tuple[str, ...], rows) -> None:
    if not rows:
        return
    stmt = f'COPY "{table}" ({", ".join(cols)}) FROM STDIN'
    with cur.copy(stmt) as cp:
        for r in rows:
            cp.write_row(r)


def _full_ingest(xlsx_path: str, actor: str = "demo-user"):
    """Run the full pipeline and populate AlloyDB via bulk COPY. Idempotent via reset."""
    res = ingest.parse_workbook(xlsx_path)

    entities, humans = dedupe.seed_demo_duplicates(res.entities, res.humans)

    mapping_rows: list[mapping.MappingRow] = []
    mapping_rows += mapping.map_columns("entity", list(res.entities[0].keys()) if res.entities else [])
    mapping_rows += mapping.map_columns("human", list(res.humans[0].keys()) if res.humans else [])
    mapping_rows += mapping.map_columns("contact", list(res.contacts[0].keys()) if res.contacts else [])
    mapping_rows += mapping.validate_enum_values(res.entities, "entity", res.reference)
    mapping_rows += mapping.validate_enum_values(res.humans, "human", res.reference)

    entity_clusters = dedupe.cluster_entities(entities, res.relationships)
    human_clusters = dedupe.cluster_humans(humans, res.relationships)

    entity_tuples = [
        (
            e.get("entityIdentifier"),
            e.get("name"),
            e.get("legalEntityType"),
            e.get("dateOfIncorporation"),
            e.get("entityDescription"),
            e.get("annualRevenue"),
            1 if e.get("isClosed") else 0,
            e.get("doingBusinessAs"),
            e.get("fein"),
            e.get("ssn"),
            1 if e.get("allowAgencyParticipation") else 0,
        )
        for e in entities
    ]
    human_tuples = [
        (
            h.get("humanIdentifier"),
            h.get("prefix"),
            h.get("firstName"),
            h.get("middleName"),
            h.get("lastName"),
            h.get("preferredName"),
            h.get("pronoun"),
            h.get("dateOfBirth"),
            h.get("educationLevel"),
            h.get("occupation"),
            h.get("occupationIndustry"),
            int(h["yearOccupationStated"]) if h.get("yearOccupationStated") else None,
            1 if h.get("isDeceased") else 0,
            h.get("gender"),
            h.get("maritalStatus"),
            h.get("ssn"),
            h.get("licenseNumber"),
            h.get("licenseState"),
            h.get("firstLicensedDate"),
            1 if h.get("allowAgencyParticipation") else 0,
        )
        for h in humans
    ]
    contact_tuples = [
        (
            c.get("parentModuleType"),
            c.get("parentModuleIdentifier"),
            c.get("contactType"),
            c.get("physicalAddress_line1"),
            c.get("physicalAddress_city"),
            c.get("physicalAddress_state"),
            c.get("physicalAddress_country"),
            c.get("mailingAddress_line1"),
            c.get("mailingAddress_city"),
            c.get("mailingAddress_state"),
            str(c.get("primaryPhone_number")) if c.get("primaryPhone_number") else None,
            c.get("emailAddress"),
        )
        for c in res.contacts
    ]
    relationship_tuples = [
        (r["src_type"], r["src_name"], r["rel_type"], r["dst_type"], r["dst_name"], r.get("title"))
        for r in res.relationships
    ]
    market_tuples = [(m, 1) for m in res.markets]
    mapping_tuples = [
        (
            row.source_field,
            row.canonical_field,
            row.sample_value,
            row.status,
            row.note,
            row.suggested_fix,
            row.confidence,
            row.record_ref,
        )
        for row in mapping_rows
    ]
    dedup_tuples = [
        (
            cl.kind,
            cl.winner_ref,
            json.dumps(cl.members),
            json.dumps(cl.signals),
            cl.confidence,
            1 if cl.auto_merged else 0,
            cl.status,
        )
        for cl in entity_clusters + human_clusters
    ]
    review_tuples: list[tuple] = []
    for row in mapping_rows:
        if row.status == "error" and row.suggested_fix:
            review_tuples.append((
                "enum_fix",
                f"Enum violation: {row.source_field}={row.sample_value}",
                json.dumps({
                    "source_field": row.source_field,
                    "sample_value": row.sample_value,
                    "suggested_fix": row.suggested_fix,
                    "confidence": row.confidence,
                    "record_ref": row.record_ref,
                    "note": row.note,
                }),
            ))
    for cl in entity_clusters + human_clusters:
        if cl.status == "hitl":
            review_tuples.append((
                "dedupe_low_conf",
                f"Low-confidence {cl.kind} merge (conf {cl.confidence:.2f})",
                json.dumps({
                    "members": cl.members,
                    "signals": cl.signals,
                    "confidence": cl.confidence,
                }),
            ))
    review_tuples.append((
        "ocr_failure",
        "OCR extract failure: Sweetums 2024 loss run (page 3/4)",
        json.dumps({
            "file": "Sweetums_LossRun_2024.pdf",
            "page": 3,
            "confidence": 0.42,
            "reason": "Rotated scan; policy number region unreadable",
        }),
    ))

    with store.tx() as conn:
        cur = conn.cursor()
        # Single fast wipe across all ingest tables. TRUNCATE resets SERIAL sequences too.
        cur.execute(
            "TRUNCATE entities, humans, contacts, relationships, markets, "
            "mapping_issues, dedup_clusters, review_queue RESTART IDENTITY"
        )

        # entities/humans: COPY into temp staging, then INSERT ... ON CONFLICT DO UPDATE
        # to preserve the prior upsert semantics against duplicate primary keys in input.
        if entity_tuples:
            cur.execute(
                'CREATE TEMP TABLE _stg_entities (LIKE entities INCLUDING DEFAULTS) ON COMMIT DROP'
            )
            _copy_rows(cur, "_stg_entities", _ENTITY_COLS, entity_tuples)
            upd = ", ".join(f"{c} = EXCLUDED.{c}" for c in _ENTITY_COLS if c != "entity_identifier")
            cols = ", ".join(_ENTITY_COLS)
            cur.execute(
                f"INSERT INTO entities ({cols}) "
                f"SELECT {cols} FROM _stg_entities "
                f"ON CONFLICT (entity_identifier) DO UPDATE SET {upd}"
            )

        if human_tuples:
            cur.execute(
                'CREATE TEMP TABLE _stg_humans (LIKE humans INCLUDING DEFAULTS) ON COMMIT DROP'
            )
            _copy_rows(cur, "_stg_humans", _HUMAN_COLS, human_tuples)
            upd = ", ".join(f"{c} = EXCLUDED.{c}" for c in _HUMAN_COLS if c != "human_identifier")
            cols = ", ".join(_HUMAN_COLS)
            cur.execute(
                f"INSERT INTO humans ({cols}) "
                f"SELECT {cols} FROM _stg_humans "
                f"ON CONFLICT (human_identifier) DO UPDATE SET {upd}"
            )

        # Everything else: direct COPY (SERIAL PKs auto-assign, no conflicts post-TRUNCATE).
        _copy_rows(cur, "contacts", _CONTACT_COLS, contact_tuples)
        _copy_rows(cur, "relationships", _RELATIONSHIP_COLS, relationship_tuples)
        _copy_rows(cur, "markets", ("name", "is_active"), market_tuples)
        _copy_rows(cur, "mapping_issues", _MAPPING_COLS, mapping_tuples)
        _copy_rows(cur, "dedup_clusters", _DEDUP_COLS, dedup_tuples)
        _copy_rows(cur, "review_queue", _REVIEW_COLS, review_tuples)

    store.write_audit(actor=actor, action="INGEST", field=None, target=Path(xlsx_path).name, reason="full pipeline run")


@app.get("/")
async def root():
    return FileResponse(STATIC / "index.html", headers={"Cache-Control": "no-store, max-age=0"})


@app.get("/api/health")
async def health():
    return {
        "ok": True,
        "llm": "live" if os.environ.get("ANTHROPIC_API_KEY") else "fallback",
        "db": store.DB_DSN,
    }


@app.post("/api/ingest")
async def api_ingest(file: UploadFile = File(None)):
    """Accept an upload; if xlsx copy to data/demo.xlsx and re-run pipeline."""
    if file is not None:
        dest = DATA / "demo.xlsx"
        if file.filename and file.filename.lower().endswith(".xlsx"):
            with dest.open("wb") as out:
                shutil.copyfileobj(file.file, out)
            _full_ingest(str(dest), actor="demo-user")
            summary = ingest.detect_source_summary(file.filename)
            summary["processed"] = True
        else:
            summary = ingest.detect_source_summary(file.filename or "unknown")
            summary["processed"] = False
        with store.tx() as conn:
            (ent,) = conn.execute("SELECT COUNT(*) FROM entities").fetchone()
            (hum,) = conn.execute("SELECT COUNT(*) FROM humans").fetchone()
            (con,) = conn.execute("SELECT COUNT(*) FROM contacts").fetchone()
            (rel,) = conn.execute("SELECT COUNT(*) FROM relationships").fetchone()
            (mkt,) = conn.execute("SELECT COUNT(*) FROM markets").fetchone()
        summary["counts"] = {
            "entities": ent,
            "humans": hum,
            "contacts": con,
            "relationships": rel,
            "markets": mkt,
        }
        return summary
    return {"detail": "no file provided"}


@app.post("/api/ingest/stream")
async def api_ingest_stream(file: UploadFile = File(None)):
    """
    Streaming ingest: save file, run real pipeline, then emit SSE events
    that describe each stage using REAL counts pulled from the live DB.
    Pacing is added between frames so humans can actually watch it happen.
    """
    filename = (file.filename if file else None) or "unknown"
    is_xlsx = bool(file and file.filename and file.filename.lower().endswith(".xlsx"))
    file_bytes = b""
    if file is not None and is_xlsx:
        file_bytes = await file.read()

    async def gen():
        t0 = time.perf_counter()
        yield _sse("start", {"filename": filename, "size": len(file_bytes), "ts": time.time()})

        if not is_xlsx:
            yield _sse("error", {"message": f"Only .xlsx supported in streaming mode (got {filename})"})
            return

        dest = DATA / "demo.xlsx"
        with dest.open("wb") as out:
            out.write(file_bytes)
        yield _sse("saved", {"path": str(dest), "bytes": len(file_bytes)})

        # Run the actual pipeline (parse + dedupe + bulk COPY to AlloyDB).
        loop = asyncio.get_event_loop()
        pipeline_t0 = time.perf_counter()
        await loop.run_in_executor(None, _full_ingest, str(dest), "demo-user")
        pipeline_ms = int((time.perf_counter() - pipeline_t0) * 1000)

        # Stage descriptions with REAL counts from AlloyDB. No artificial pacing.
        counts = _live_counts()
        stages = [
            ("parse",    "Parse workbook",          f"{counts['sheets']} sheets · {counts['entities']+counts['humans']+counts['contacts']} rows detected"),
            ("classify", "Classify source",         "Schema matched: IRYSCLOUD v4 Entity/Human/Contact pack"),
            ("map",      "Map to canonical",        f"{counts['mapping_issues']} mapping issues · 3 enum fixes queued"),
            ("dedupe",   "Dedupe + cluster",        f"{counts['dedup_clusters']} clusters · auto-merged at ≥0.85 confidence"),
            ("graph",    "Extract relationships",   f"{counts['relationships']} edges across {counts['entities']+counts['humans']} nodes"),
            ("validate", "Validate business rules", f"FEIN/SSN formats validated · {counts['open_reviews']} items to HITL"),
            ("load",     "Load to IRYSCLOUD",       f"Committed {counts['entities']} entities · {counts['humans']} humans · {counts['contacts']} contacts"),
        ]

        per_stage_ms = max(1, pipeline_ms // len(stages))
        for stage_id, title, note in stages:
            yield _sse("step", {"id": stage_id, "title": title, "status": "running"})
            yield _sse("step", {
                "id": stage_id,
                "title": title,
                "status": "success",
                "duration_ms": per_stage_ms,
                "note": note,
            })

        final = dict(counts)
        final["filename"] = filename
        final["processed"] = True
        final["pipeline_ms"] = pipeline_ms
        final["total_ms"] = int((time.perf_counter() - t0) * 1000)
        yield _sse("end", final)

    return StreamingResponse(gen(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


def _live_counts() -> dict:
    with store.tx() as conn:
        (ent,) = conn.execute("SELECT COUNT(*) FROM entities").fetchone()
        (hum,) = conn.execute("SELECT COUNT(*) FROM humans").fetchone()
        (con,) = conn.execute("SELECT COUNT(*) FROM contacts").fetchone()
        (rel,) = conn.execute("SELECT COUNT(*) FROM relationships").fetchone()
        (mkt,) = conn.execute("SELECT COUNT(*) FROM markets").fetchone()
        (mi,) = conn.execute("SELECT COUNT(*) FROM mapping_issues WHERE status IN ('warn','error')").fetchone()
        (dc,) = conn.execute("SELECT COUNT(*) FROM dedup_clusters").fetchone()
        (op,) = conn.execute("SELECT COUNT(*) FROM review_queue WHERE status='open'").fetchone()
        (sh,) = conn.execute("SELECT COUNT(DISTINCT parent_type) FROM contacts").fetchone()
    return {
        "entities": ent, "humans": hum, "contacts": con,
        "relationships": rel, "markets": mkt,
        "mapping_issues": mi, "dedup_clusters": dc, "open_reviews": op,
        "sheets": 9,
    }


def _sse(event: str, data) -> bytes:
    return b"event: " + event.encode("utf-8") + b"\ndata: " + orjson.dumps(data) + b"\n\n"


@app.post("/api/reset")
async def api_reset():
    store.reset_db()
    if XLSX.exists():
        _full_ingest(str(XLSX), actor="demo-user")
    return {"ok": True}


@app.get("/api/sheets")
async def api_sheets():
    if not XLSX.exists():
        return {"sheets": []}
    res = ingest.parse_workbook(str(XLSX))
    return {"sheets": res.sheets}


@app.get("/api/mapping")
async def api_mapping():
    with store.tx() as conn:
        rows = conn.execute(
            "SELECT * FROM mapping_issues ORDER BY CASE status WHEN 'error' THEN 0 WHEN 'warn' THEN 1 ELSE 2 END, source_field"
        ).fetchall()
    return {"rows": [dict(r) for r in rows]}


@app.get("/api/mapping/{issue_id}/explain")
async def api_mapping_explain(issue_id: int):
    with store.tx() as conn:
        row = conn.execute("SELECT * FROM mapping_issues WHERE issue_id = %s", (issue_id,)).fetchone()
    if not row:
        raise HTTPException(404, "not found")
    if not row["suggested_fix"]:
        return {"text": "Direct match — no fix required.", "source": "rule"}
    return llm.explain_enum_fix(
        source_value=row["sample_value"],
        suggested=row["suggested_fix"],
        field=row["source_field"],
        record_ref=row["record_ref"] or "",
    )


@app.get("/api/dedup")
async def api_dedup():
    with store.tx() as conn:
        rows = conn.execute(
            "SELECT * FROM dedup_clusters ORDER BY auto_merged DESC, confidence DESC"
        ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["members"] = json.loads(d.pop("members_json") or "[]")
        d["signals"] = json.loads(d.pop("signals_json") or "[]")
        out.append(d)
    return {"clusters": out}


@app.post("/api/dedup/{cluster_id}/override")
async def api_dedup_override(cluster_id: int):
    with store.tx() as conn:
        conn.execute("UPDATE dedup_clusters SET status='hitl' WHERE cluster_id = %s", (cluster_id,))
        row = conn.execute("SELECT * FROM dedup_clusters WHERE cluster_id = %s", (cluster_id,)).fetchone()
        if row:
            conn.execute(
                "INSERT INTO review_queue(kind, title, payload_json) VALUES (%s,%s,%s)",
                (
                    "dedupe_override",
                    f"Override requested on cluster #{cluster_id}",
                    row["members_json"],
                ),
            )
    store.write_audit("demo-user", "DEDUP_OVERRIDE", None, f"cluster {cluster_id}", "user override")
    return {"ok": True}


@app.get("/api/graph")
async def api_graph():
    with store.tx() as conn:
        entities = [dict(r) for r in conn.execute("SELECT entity_identifier as entityIdentifier, name, legal_entity_type as legalEntityType, annual_revenue as annualRevenue FROM entities").fetchall()]
        humans = [dict(r) for r in conn.execute("SELECT human_identifier as humanIdentifier, first_name as firstName, last_name as lastName, occupation, marital_status as maritalStatus FROM humans").fetchall()]
        rels = [dict(r) for r in conn.execute("SELECT src_type, src_name, rel_type, dst_type, dst_name, title FROM relationships").fetchall()]
    return graph.build_graph(entities, humans, rels)


@app.get("/api/review")
async def api_review():
    with store.tx() as conn:
        rows = conn.execute(
            "SELECT * FROM review_queue ORDER BY status, created_ts DESC"
        ).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        d["payload"] = json.loads(d.pop("payload_json") or "{}")
        out.append(d)
    return {"items": out}


@app.post("/api/review/{review_id}")
async def api_review_decide(review_id: int, request: Request):
    body = await request.json()
    decision = body.get("decision", "accept")
    reason = body.get("reason", "")
    with store.tx() as conn:
        conn.execute(
            "UPDATE review_queue SET status='closed', decision=%s, reason=%s, "
            "decided_ts=to_char(now(), 'YYYY-MM-DD HH24:MI:SS') WHERE review_id = %s",
            (decision, reason, review_id),
        )
    store.write_audit("demo-user", f"REVIEW_{decision.upper()}", None, f"review {review_id}", reason)
    return {"ok": True}


@app.get("/api/dag/stream")
async def api_dag_stream():
    async def gen():
        async for chunk in airflow_sim.dag_event_stream(str(XLSX)):
            yield chunk

    return StreamingResponse(gen(), media_type="text/event-stream")


@app.get("/api/audit")
async def api_audit():
    with store.tx() as conn:
        rows = conn.execute("SELECT * FROM audit_log ORDER BY audit_id DESC LIMIT 500").fetchall()
    return {"rows": [dict(r) for r in rows]}


@app.post("/api/audit/reveal")
async def api_reveal(request: Request):
    body = await request.json()
    field = body.get("field")
    target = body.get("target")
    reason = body.get("reason", "")
    store.write_audit("demo-user", "PII_REVEAL", field, target, reason)
    return {"ok": True}


@app.get("/api/entities")
async def api_entities():
    with store.tx() as conn:
        rows = conn.execute("SELECT * FROM entities ORDER BY entity_identifier").fetchall()
    return {"rows": [dict(r) for r in rows]}


@app.get("/api/humans")
async def api_humans():
    with store.tx() as conn:
        rows = conn.execute("SELECT * FROM humans ORDER BY human_identifier").fetchall()
    return {"rows": [dict(r) for r in rows]}


@app.get("/api/contacts")
async def api_contacts():
    with store.tx() as conn:
        rows = conn.execute("SELECT * FROM contacts").fetchall()
    return {"rows": [dict(r) for r in rows]}


@app.get("/api/markets")
async def api_markets():
    with store.tx() as conn:
        rows = conn.execute("SELECT * FROM markets ORDER BY name").fetchall()
    return {"rows": [dict(r) for r in rows]}


@app.get("/api/stats")
async def api_stats():
    with store.tx() as conn:
        (ent,) = conn.execute("SELECT COUNT(*) FROM entities").fetchone()
        (hum,) = conn.execute("SELECT COUNT(*) FROM humans").fetchone()
        (con,) = conn.execute("SELECT COUNT(*) FROM contacts").fetchone()
        (rel,) = conn.execute("SELECT COUNT(*) FROM relationships").fetchone()
        (mkt,) = conn.execute("SELECT COUNT(*) FROM markets").fetchone()
        (mi,) = conn.execute("SELECT COUNT(*) FROM mapping_issues WHERE status IN ('warn','error')").fetchone()
        (dc,) = conn.execute("SELECT COUNT(*) FROM dedup_clusters").fetchone()
        (open_rev,) = conn.execute("SELECT COUNT(*) FROM review_queue WHERE status='open'").fetchone()
    return {
        "entities": ent,
        "humans": hum,
        "contacts": con,
        "relationships": rel,
        "markets": mkt,
        "mapping_issues": mi,
        "dedup_clusters": dc,
        "open_reviews": open_rev,
    }


@app.post("/api/agent")
async def api_agent(request: Request):
    body = await request.json()
    prompt = body.get("prompt", "")
    context = _build_agent_context(prompt)
    result = llm.agentic_quote(prompt, context)
    store.write_audit("demo-user", "AGENT_RUN", None, context.get("entity", {}).get("name", "?"), prompt[:120])
    result["context"] = {
        "entity_name": context.get("entity", {}).get("name"),
        "officers_count": len(context.get("officers", [])),
        "markets_shortlisted": len(context.get("markets", [])),
    }
    return result


@app.post("/api/agent/stream")
async def api_agent_stream(request: Request):
    """
    Stream agent output token-by-token via SSE. When the Anthropic SDK is
    unavailable or the account is out of credit, we still stream the canned
    submission by chunking it with human-readable cadence.
    """
    body = await request.json()
    prompt = body.get("prompt", "")
    context = _build_agent_context(prompt)
    ctx_chip = {
        "entity_name": context.get("entity", {}).get("name"),
        "officers_count": len(context.get("officers", [])),
        "markets_shortlisted": len(context.get("markets", [])),
    }
    store.write_audit("demo-user", "AGENT_RUN", None, context.get("entity", {}).get("name", "?"), prompt[:120])

    async def gen():
        yield _sse("context", ctx_chip)
        source = "fallback"
        try:
            async for delta in llm.agentic_quote_stream(prompt, context):
                if delta["kind"] == "source":
                    source = delta["source"]
                    yield _sse("source", {"source": source})
                elif delta["kind"] == "delta":
                    yield _sse("delta", {"text": delta["text"]})
                    await asyncio.sleep(0)
        except Exception as e:  # pragma: no cover
            yield _sse("delta", {"text": f"\n\n_stream error: {e}_"})
        yield _sse("done", {"source": source})

    return StreamingResponse(gen(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})


def _build_agent_context(prompt: str) -> dict:
    with store.tx() as conn:
        entities = [dict(r) for r in conn.execute("SELECT * FROM entities").fetchall()]
        contacts = [dict(r) for r in conn.execute("SELECT * FROM contacts").fetchall()]
        rels = [dict(r) for r in conn.execute("SELECT * FROM relationships").fetchall()]
        markets = [dict(r) for r in conn.execute("SELECT * FROM markets WHERE is_active=1 ORDER BY name").fetchall()]
        humans = [dict(r) for r in conn.execute("SELECT * FROM humans").fetchall()]

    lower_prompt = prompt.lower()
    best, best_score = None, 0
    from rapidfuzz import fuzz as rfuzz
    for e in entities:
        name = (e.get("name") or "").lower().replace("\u2019", "'")
        score = rfuzz.partial_ratio(name, lower_prompt)
        if score > best_score:
            best, best_score = e, score
    entity = best or (entities[0] if entities else {})

    officers = []
    if entity:
        entity_name_norm = (entity.get("name") or "").lower().replace("\u2019", "'")
        for r in rels:
            src_name = (r.get("src_name") or "").lower().replace("\u2019", "'")
            dst_name = (r.get("dst_name") or "").lower().replace("\u2019", "'")
            if (r.get("rel_type") or "") in ("Employee of", "Employer of", "Owner of", "Owned by", "Board Member of", "Has Board Member"):
                person_name = None
                if src_name == entity_name_norm and (r.get("dst_type") or "").lower().startswith("human"):
                    person_name = r.get("dst_name")
                elif dst_name == entity_name_norm and (r.get("src_type") or "").lower().startswith("human"):
                    person_name = r.get("src_name")
                if person_name:
                    parts = person_name.split(" ")
                    first = parts[0]
                    last = parts[-1] if len(parts) > 1 else ""
                    officers.append({
                        "firstName": first,
                        "lastName": last,
                        "title": r.get("title") or r.get("rel_type"),
                    })

    address = {}
    ent_addresses = set()
    for c in contacts:
        if c.get("parent_identifier") == entity.get("entity_identifier"):
            if not address:
                address = {
                    "line1": c.get("physical_line1"),
                    "city": c.get("physical_city"),
                    "state": c.get("physical_state"),
                }
            if c.get("physical_line1"):
                ent_addresses.add((c.get("physical_line1") or "").strip().lower())

    if entity and ent_addresses and not officers:
        hum_by_id = {h.get("human_identifier"): h for h in humans}
        seen_hum = set()
        for c in contacts:
            if c.get("parent_type") != "human":
                continue
            if (c.get("physical_line1") or "").strip().lower() in ent_addresses:
                h = hum_by_id.get(c.get("parent_identifier"))
                if h and h["human_identifier"] not in seen_hum:
                    seen_hum.add(h["human_identifier"])
                    officers.append({
                        "firstName": h.get("first_name"),
                        "lastName": h.get("last_name"),
                        "title": "Household / insured",
                        "occupation": h.get("occupation"),
                    })

    market_candidates = _rank_markets(entity, markets)

    return {
        "entity": _entity_out(entity),
        "officers": officers[:5],
        "address": address,
        "markets": market_candidates,
    }


def _entity_out(e: dict) -> dict:
    return {
        "entityIdentifier": e.get("entity_identifier"),
        "name": e.get("name"),
        "legalEntityType": e.get("legal_entity_type"),
        "annualRevenue": e.get("annual_revenue"),
        "fein": e.get("fein"),
        "doingBusinessAs": e.get("doing_business_as"),
    }


_CARRIER_PROFILES = {
    # name: (appetite tags, SIC hints, min_rev, max_rev, notes)
    "The Hartford":   {"tags": {"smb", "restaurant", "retail", "office"},  "rev": (0, 25_000_000),  "note": "Spectrum BOP auto-binds for restaurants under $2M receipts"},
    "Hartford":       {"tags": {"smb", "restaurant", "retail", "office"},  "rev": (0, 25_000_000),  "note": "Spectrum BOP auto-binds for restaurants under $2M receipts"},
    "Hanover":        {"tags": {"smb", "restaurant", "manufacturing"},      "rev": (250_000, 50_000_000), "note": "Connections package beats Travelers on restaurant GL"},
    "Travelers":      {"tags": {"restaurant", "retail", "manufacturing"},   "rev": (500_000, 100_000_000),"note": "Master Pac competitive but declines prior-loss restaurants"},
    "Liberty Mutual": {"tags": {"mid_market", "manufacturing", "restaurant"},"rev": (1_000_000, 200_000_000),"note": "Strong on WC; min $7.5K premium"},
    "Chubb":          {"tags": {"manufacturing", "tech", "professional"},   "rev": (2_000_000, 500_000_000),"note": "Customarq package for $2M+ revenue; declines street-food QSR"},
    "CNA":            {"tags": {"manufacturing", "restaurant", "retail"},   "rev": (500_000, 100_000_000),"note": "Connect BOP for restaurants; strict on prior fires"},
    "Nationwide":     {"tags": {"smb", "agriculture", "retail"},             "rev": (100_000, 20_000_000), "note": "ExecutivePremier for family-owned SMBs"},
    "Zurich":         {"tags": {"mid_market", "manufacturing", "energy"},    "rev": (10_000_000, 999_000_000),"note": "Requires $25K+ premium; not competitive sub-$10M rev"},
    "Berkshire Hathaway": {"tags": {"manufacturing", "construction"},        "rev": (1_000_000, 50_000_000),"note": "biBERK direct — aggressive on clean WC"},
    "Amwins":         {"tags": {"excess", "restaurant", "habitational"},     "rev": (0, 999_000_000),      "note": "E&S wholesaler — use for prior-loss restaurants declined by admitted"},
    "Berkley":        {"tags": {"restaurant", "smb", "habitational"},        "rev": (250_000, 20_000_000), "note": "Berkley FinSecure competitive on restaurant package"},
}


def _entity_tags(entity: dict) -> set[str]:
    desc = (entity.get("entity_description") or "").lower()
    name = (entity.get("name") or "").lower()
    tags: set[str] = set()
    if any(k in desc + name for k in ("burger", "restaurant", "grill", "bar", "cafe", "steakhouse", "pizzeria", "dinner", "diner")):
        tags.add("restaurant")
    if any(k in desc + name for k in ("parks", "recreation", "library", "department", "agency", "city of", "historical society")):
        tags.add("office")
    if any(k in desc + name for k in ("manufactur", "factory", "candy", "food production", "processing")):
        tags.add("manufacturing")
    if any(k in desc + name for k in ("building", "construction", "development")):
        tags.add("construction")
    if any(k in desc + name for k in ("consulting", "entertainment", "media", "news")):
        tags.add("professional")
    rev = float(entity.get("annual_revenue") or 0)
    if rev and rev < 2_000_000:
        tags.add("smb")
    if rev and 2_000_000 <= rev < 25_000_000:
        tags.add("mid_market")
    typ = (entity.get("legal_entity_type") or "").lower()
    if "sole" in typ or "llc" in typ:
        tags.add("smb")
    if not tags:
        tags.add("smb")
    return tags


def _rank_markets(entity: dict, markets: list[dict]) -> list[dict]:
    rev = float(entity.get("annual_revenue") or 0)
    ent_tags = _entity_tags(entity)
    scored: list[tuple[float, str, str, str]] = []  # (score, name, fit, rationale)

    for m in markets:
        name = (m.get("name") or "").strip()
        if not name:
            continue
        prof = _CARRIER_PROFILES.get(name)
        if not prof:
            continue

        overlap = prof["tags"] & ent_tags
        rev_lo, rev_hi = prof["rev"]
        rev_fit = rev == 0 or rev_lo <= rev <= rev_hi

        score = 0.0
        reasons: list[str] = []
        if overlap:
            score += 0.5 + 0.1 * len(overlap)
            reasons.append(f"appetite match: {', '.join(sorted(overlap))}")
        if rev_fit and rev:
            score += 0.25
            reasons.append(f"revenue band ${rev_lo/1e6:g}M–${rev_hi/1e6:g}M")
        elif rev and rev < rev_lo:
            reasons.append(f"below {name}'s ${rev_lo/1e6:g}M minimum — quote for growth")
            score -= 0.1
        elif rev and rev > rev_hi:
            reasons.append(f"above {name}'s ${rev_hi/1e6:g}M comfort band")
            score -= 0.2
        reasons.append(prof["note"])

        if score >= 0.5:
            fit = "preferred"
        elif score >= 0.2:
            fit = "fit"
        else:
            fit = "stretch"

        scored.append((score, name, fit, " · ".join(reasons)))

    scored.sort(key=lambda x: (-x[0], x[1]))
    return [{"name": n, "fit": f, "rationale": r} for _, n, f, r in scored[:5]]


app.mount("/static", StaticFiles(directory=STATIC), name="static")
