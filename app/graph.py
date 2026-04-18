from __future__ import annotations

from rapidfuzz import fuzz


REL_COLORS = {
    "Spouse of": "#be185d",
    "Child of": "#9333ea",
    "Parent of": "#9333ea",
    "Sibling of": "#9333ea",
    "Employee of": "#0ea5e9",
    "Employer of": "#0ea5e9",
    "Co-Worker of": "#16a34a",
    "Board Member of": "#f59e0b",
    "Has Board Member": "#f59e0b",
    "Owner of": "#ef4444",
    "Owned by": "#ef4444",
    "Subsidiary of": "#64748b",
}


def _norm(s: str) -> str:
    return (
        (s or "")
        .replace("\u2019", "'")
        .replace("\u2018", "'")
        .strip()
        .lower()
    )


def _canonical_name(name: str, known: dict[str, str]) -> str:
    """Map free-text name to a canonical name already seen in entities/humans if close enough."""
    n = _norm(name)
    if n in known:
        return known[n]
    best, best_score = None, 0
    for k, v in known.items():
        score = fuzz.ratio(n, k)
        if score > best_score:
            best, best_score = v, score
    if best and best_score >= 85:
        return best
    return name


def build_graph(entities: list[dict], humans: list[dict], relationships: list[dict]) -> dict:
    known_entities = {_norm(e.get("name")): e.get("name") for e in entities}
    known_humans = {
        _norm(f"{h.get('firstName','')} {h.get('lastName','')}".strip()): f"{h.get('firstName','')} {h.get('lastName','')}".strip()
        for h in humans
    }

    nodes: dict[str, dict] = {}
    edges: list[dict] = []

    for e in entities:
        key = f"E::{e.get('name')}"
        nodes[key] = {
            "id": key,
            "label": e.get("name"),
            "group": "entity",
            "title": f"Entity · {e.get('legalEntityType') or ''} · rev ${int(e.get('annualRevenue') or 0):,}",
            "shape": "box",
            "color": {"background": "#1e293b", "border": "#6366f1"},
            "font": {"color": "#e2e8f0"},
        }

    for h in humans:
        full = f"{h.get('firstName','')} {h.get('lastName','')}".strip()
        key = f"H::{full}"
        nodes[key] = {
            "id": key,
            "label": full,
            "group": "human",
            "title": f"Human · {h.get('occupation') or ''} · {h.get('maritalStatus') or ''}",
            "shape": "ellipse",
            "color": {"background": "#eef2ff", "border": "#6366f1"},
            "font": {"color": "#1e1b4b"},
        }

    for r in relationships:
        src_type = (r["src_type"] or "").lower()
        dst_type = (r["dst_type"] or "").lower()
        if src_type == "opportunity" or dst_type == "opportunity":
            continue
        if src_type.startswith("human"):
            name = _canonical_name(r["src_name"], known_humans)
            src_key = f"H::{name}"
        else:
            name = _canonical_name(r["src_name"], known_entities)
            src_key = f"E::{name}"
        if dst_type.startswith("human"):
            name = _canonical_name(r["dst_name"], known_humans)
            dst_key = f"H::{name}"
        else:
            name = _canonical_name(r["dst_name"], known_entities)
            dst_key = f"E::{name}"

        if src_key not in nodes:
            label = src_key.split("::", 1)[1]
            nodes[src_key] = {
                "id": src_key,
                "label": label,
                "group": "human" if src_key.startswith("H::") else "entity",
                "shape": "ellipse" if src_key.startswith("H::") else "box",
                "color": {"background": "#fff7ed", "border": "#f59e0b"},
                "title": "Referenced in relationship but missing from master record",
            }
        if dst_key not in nodes:
            label = dst_key.split("::", 1)[1]
            nodes[dst_key] = {
                "id": dst_key,
                "label": label,
                "group": "human" if dst_key.startswith("H::") else "entity",
                "shape": "ellipse" if dst_key.startswith("H::") else "box",
                "color": {"background": "#fff7ed", "border": "#f59e0b"},
                "title": "Referenced in relationship but missing from master record",
            }

        edges.append(
            {
                "from": src_key,
                "to": dst_key,
                "label": r["rel_type"],
                "title": (r.get("title") or r["rel_type"]),
                "arrows": "to",
                "color": {"color": REL_COLORS.get(r["rel_type"], "#94a3b8"), "opacity": 0.7},
                "font": {"color": "#475569", "size": 10, "strokeWidth": 0},
                "smooth": {"type": "dynamic"},
            }
        )

    return {"nodes": list(nodes.values()), "edges": edges}
