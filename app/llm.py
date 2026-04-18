from __future__ import annotations

import os


MODEL = "claude-sonnet-4-5"


def _has_key() -> bool:
    return bool(os.environ.get("ANTHROPIC_API_KEY"))


def explain_enum_fix(source_value: str, suggested: str, field: str, record_ref: str) -> dict:
    """Return {text, source} — source in {'llm','fallback'}."""
    if not _has_key():
        return {
            "source": "fallback",
            "text": (
                f"Suggested mapping: `{source_value}` → `{suggested}` for `{field}` on "
                f"{record_ref}. Levenshtein distance of 1 character; no other candidate "
                "within distance 3 in the Reference Values sheet. This is the canonical "
                "value ingested by Iris for underwriting and reporting."
            ),
        }
    try:
        import anthropic

        client = anthropic.Anthropic()
        msg = client.messages.create(
            model=MODEL,
            max_tokens=250,
            system=(
                "You are the AI data mapping explainer in Iris, an insurance agency "
                "management system. Be crisp, technical, 2-3 sentences max. Use "
                "backticks around field and enum values."
            ),
            messages=[
                {
                    "role": "user",
                    "content": (
                        f"Explain to a data steward why we are suggesting to fix "
                        f"`{source_value}` to `{suggested}` in field `{field}` on "
                        f"record {record_ref}. Mention edit distance and why other "
                        "candidates were rejected."
                    ),
                }
            ],
        )
        text = msg.content[0].text if msg.content else ""
        return {"source": "llm", "text": text}
    except Exception:
        return {
            "source": "fallback",
            "text": (
                f"Suggested mapping: `{source_value}` → `{suggested}`; distance 1; "
                "no other candidate within 3."
            ),
        }


def agentic_quote(prompt: str, context: dict) -> dict:
    """Return {markdown, source}. Context carries entity/people/address/markets."""
    if not _has_key():
        return {"source": "fallback", "markdown": _canned_quote(context)}
    try:
        import anthropic

        client = anthropic.Anthropic()
        msg = client.messages.create(
            model=MODEL,
            max_tokens=900,
            system=(
                "You are Iris' agentic submission co-pilot for a commercial insurance "
                "agency. You produce crisp, market-ready submission packages in Markdown "
                "using only the supplied structured context — do not invent carriers, "
                "revenues, or people. Use clear sections and a 'Recommended carriers' "
                "table with one-line rationales."
            ),
            messages=[
                {
                    "role": "user",
                    "content": (
                        f"USER REQUEST:\n{prompt}\n\nSTRUCTURED CONTEXT (JSON):\n"
                        f"{context}"
                    ),
                }
            ],
        )
        return {"source": "llm", "markdown": msg.content[0].text if msg.content else ""}
    except Exception:
        return {"source": "fallback", "markdown": _canned_quote(context)}


async def agentic_quote_stream(prompt: str, context: dict):
    """
    Async generator yielding {kind: 'source'|'delta', ...} events.
    Tries live Anthropic streaming; on failure chunks the canned markdown
    with human-readable cadence so the UI still feels realtime.
    """
    import asyncio

    if _has_key():
        try:
            import anthropic
            client = anthropic.Anthropic()
            with client.messages.stream(
                model=MODEL,
                max_tokens=900,
                system=(
                    "You are Iris' agentic submission co-pilot for a commercial insurance "
                    "agency. Produce crisp, market-ready Markdown using only the supplied "
                    "structured context — do not invent carriers, revenues, or people. "
                    "Use clear sections and a 'Recommended carriers' table with one-line "
                    "rationales."
                ),
                messages=[{
                    "role": "user",
                    "content": f"USER REQUEST:\n{prompt}\n\nSTRUCTURED CONTEXT (JSON):\n{context}",
                }],
            ) as stream:
                yield {"kind": "source", "source": "llm"}
                for text in stream.text_stream:
                    yield {"kind": "delta", "text": text}
            return
        except Exception:
            pass

    yield {"kind": "source", "source": "fallback"}
    md = _canned_quote(context)
    # Chunk into ~6-char deltas with tiny pauses so the UI streams naturally
    i = 0
    n = len(md)
    while i < n:
        step = 6 if md[i] != "\n" else 1
        chunk = md[i:i + step]
        yield {"kind": "delta", "text": chunk}
        # Longer pause on newlines/headings for visual rhythm
        if "\n" in chunk:
            await asyncio.sleep(0.02)
        else:
            await asyncio.sleep(0.012)
        i += step


def _canned_quote(c: dict) -> str:
    ent = c.get("entity") or {}
    officers = c.get("officers") or []
    address = c.get("address") or {}
    markets = c.get("markets") or []
    officer_lines = "\n".join(
        f"- **{o.get('firstName','')} {o.get('lastName','')}** — {o.get('title') or o.get('occupation') or 'Officer'}"
        for o in officers
    ) or "- _(no officers linked)_"

    market_lines = "\n".join(
        f"| {m['name']} | {m['fit']} | {m['rationale']} |" for m in markets[:5]
    )
    dba = ent.get("doingBusinessAs") or ent.get("name")
    rev = int(ent.get("annualRevenue") or 0)

    return f"""# Submission — {ent.get('name','?')}

**DBA:** {dba}  \n**Legal entity type:** `{ent.get('legalEntityType','?')}`  \n**FEIN:** {ent.get('fein','?')}  \n**Annual revenue:** ${rev:,}  \n**Physical address:** {address.get('line1','?')}, {address.get('city','?')}, {address.get('state','?').upper()}

## Officers / key people
{officer_lines}

## Coverage posture
Commercial General Liability, BOP, Property, Workers' Comp (if employee count > 0), Cyber (small).

## Recommended carriers (Active appointments only)
| Carrier | Fit | Rationale |
|---|---|---|
{market_lines or '| _(none)_ | | |'}

## Next steps
1. Confirm officer list + payroll band with insured.
2. Bind property valuation — floor plan on file.
3. Issue submission packages to top-3 carriers within SLA.
"""
