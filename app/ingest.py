from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Any

from python_calamine import CalamineWorkbook


def _clean(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, str):
        s = v.strip()
        return s if s else None
    if isinstance(v, (datetime, date)):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, float) and v.is_integer():
        # Preserve integer-valued floats that came from Excel numeric cells
        return v
    return v


def _as_cell(v: Any) -> Any:
    """Normalize raw calamine cell values: empty string → None."""
    if v is None:
        return None
    if isinstance(v, str) and not v.strip():
        return None
    return v


SHEET_KIND = {
    "Entity Template": "template_entity",
    "Human Template": "template_human",
    "Contact Template": "template_contact",
    "Reference Values": "reference",
    "Entity Demo Data": "entities",
    "Human Demo Data": "humans",
    "Contact Demo Data": "contacts",
    "Relationship Demo Data": "relationships",
    "Market Demo Data": "markets",
}


@dataclass
class IngestResult:
    entities: list[dict] = field(default_factory=list)
    humans: list[dict] = field(default_factory=list)
    contacts: list[dict] = field(default_factory=list)
    relationships: list[dict] = field(default_factory=list)
    markets: list[str] = field(default_factory=list)
    reference: dict[str, list[str]] = field(default_factory=dict)
    sheets: list[dict] = field(default_factory=list)


def _parse_reference(rows: list[list[Any]]) -> dict[str, list[str]]:
    current: str | None = None
    cats: dict[str, list[str]] = {}
    for row in rows[1:] if len(rows) > 1 else []:
        cat = _as_cell(row[0]) if len(row) > 0 else None
        val = _as_cell(row[1]) if len(row) > 1 else None
        if cat:
            current = str(cat).strip()
            cats.setdefault(current, [])
        if val and current:
            cats[current].append(str(val).strip())
    return cats


def _parse_tabular(rows: list[list[Any]]) -> list[dict]:
    if len(rows) < 2:
        return []
    headers = [_as_cell(h) for h in rows[0]]
    out: list[dict] = []
    for r in rows[1:]:
        if not r or _as_cell(r[0]) is None:
            continue
        row = {
            headers[i]: _clean(r[i])
            for i in range(len(headers))
            if i < len(r) and headers[i]
        }
        out.append(row)
    return out


def _parse_relationships(rows: list[list[Any]]) -> list[dict]:
    out: list[dict] = []
    for r in rows[1:] if len(rows) > 1 else []:
        padded = list(r) + [None] * max(0, 6 - len(r))
        src_type, src_name, rel, dst_type, dst_name, title = padded[:6]
        if not _as_cell(src_name) or not _as_cell(dst_name) or not _as_cell(rel):
            continue
        out.append(
            {
                "src_type": str(src_type).strip() if src_type else None,
                "src_name": str(src_name).strip(),
                "rel_type": str(rel).strip(),
                "dst_type": str(dst_type).strip() if dst_type else None,
                "dst_name": str(dst_name).strip(),
                "title": str(title).strip() if title else None,
            }
        )
    return out


def _parse_markets(rows: list[list[Any]]) -> list[str]:
    return [
        str(r[0]).strip()
        for r in (rows[1:] if len(rows) > 1 else [])
        if r and _as_cell(r[0]) is not None
    ]


def parse_workbook(xlsx_path: str | Path) -> IngestResult:
    wb = CalamineWorkbook.from_path(str(xlsx_path))
    result = IngestResult()
    for name in wb.sheet_names:
        sheet = wb.get_sheet_by_name(name)
        rows = sheet.to_python()
        kind = SHEET_KIND.get(name, "unknown")
        result.sheets.append(
            {
                "name": name,
                "kind": kind,
                "rows": max(0, len(rows) - 1),
                "cols": len(rows[0]) if rows else 0,
            }
        )
        if kind == "reference":
            result.reference = _parse_reference(rows)
        elif kind == "entities":
            result.entities = _parse_tabular(rows)
        elif kind == "humans":
            result.humans = _parse_tabular(rows)
        elif kind == "contacts":
            result.contacts = _parse_tabular(rows)
        elif kind == "relationships":
            result.relationships = _parse_relationships(rows)
        elif kind == "markets":
            result.markets = _parse_markets(rows)
    return result


def detect_source_summary(filename: str) -> dict:
    lower = filename.lower()
    if lower.endswith(".xlsx"):
        return {"type": "xlsx", "engine": "calamine", "note": "structured workbook"}
    if lower.endswith(".csv"):
        return {"type": "csv", "engine": "calamine", "note": "delimited text"}
    if lower.endswith(".pdf"):
        return {"type": "pdf", "engine": "ocr-preview", "note": "simulated OCR extract"}
    if lower.endswith(".eml"):
        return {"type": "email", "engine": "mime-preview", "note": "simulated email parse"}
    return {"type": "unknown", "engine": "generic", "note": "no parser matched"}
