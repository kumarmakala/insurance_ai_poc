from __future__ import annotations

import copy
from dataclasses import dataclass, field
from typing import Iterable

from rapidfuzz import fuzz
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


@dataclass
class Cluster:
    kind: str  # entity | human
    winner_ref: str
    members: list[dict]
    signals: list[str]
    confidence: float
    auto_merged: bool
    status: str = "auto"  # auto | hitl


def _normalize_name(s: str | None) -> str:
    if not s:
        return ""
    return (
        s.replace("\u2019", "'")
        .replace("\u2018", "'")
        .lower()
        .strip()
    )


def _entity_blob(r: dict) -> str:
    return " ".join(
        filter(
            None,
            [
                _normalize_name(r.get("name")),
                _normalize_name(r.get("doingBusinessAs")),
                (r.get("entityDescription") or "").lower()[:120],
            ],
        )
    )


def _human_blob(r: dict) -> str:
    return " ".join(
        filter(
            None,
            [
                _normalize_name(r.get("firstName")),
                _normalize_name(r.get("middleName")),
                _normalize_name(r.get("lastName")),
                _normalize_name(r.get("preferredName")),
            ],
        )
    )


def seed_demo_duplicates(entities: list[dict], humans: list[dict]) -> tuple[list[dict], list[dict]]:
    """Inject visible dup scenarios so the demo always has the three featured cases."""
    entities = copy.deepcopy(entities)
    humans = copy.deepcopy(humans)

    bob = next((e for e in entities if "coastal" in (e.get("name") or "").lower() and "burger" in (e.get("name") or "").lower()), None)
    if bob:
        clone1 = copy.deepcopy(bob)
        clone1["entityIdentifier"] = "ent001b"
        clone1["name"] = "Coastal Grille Burgers"
        clone1["doingBusinessAs"] = None
        clone1["entityDescription"] = "Family burger spot in Annapolis."
        clone2 = copy.deepcopy(bob)
        clone2["entityIdentifier"] = "ent001c"
        clone2["name"] = "Coastal Grille & Burgers LLC"
        clone2["legalEntityType"] = "llc"
        clone2["entityDescription"] = "Legal-entity record for Coastal Grille & Burgers."
        entities.extend([clone1, clone2])

    ron = next((h for h in humans if (h.get("lastName") or "").lower() == "donovan" and (h.get("firstName") or "").lower() == "ronald"), None)
    if ron:
        c1 = copy.deepcopy(ron)
        c1["humanIdentifier"] = "hum008b"
        c1["firstName"] = "Ron"
        c1["middleName"] = None
        c1["preferredName"] = "Ron"
        c1["occupation"] = ron.get("occupation")
        c2 = copy.deepcopy(ron)
        c2["humanIdentifier"] = "hum008c"
        c2["firstName"] = "R."
        c2["middleName"] = "U."
        c2["preferredName"] = None
        humans.extend([c1, c2])

    # HITL low-confidence case: a "John Smith" variant on a different DOB / no shared signals
    hitl_a = {
        "humanIdentifier": "hum901",
        "firstName": "John",
        "lastName": "Smith",
        "dateOfBirth": "1978-02-01",
        "preferredName": "Johnny",
        "occupation": "sales_Marketing",
        "occupationIndustry": "retail",
        "allowAgencyParticipation": True,
    }
    hitl_b = {
        "humanIdentifier": "hum902",
        "firstName": "Jon",
        "lastName": "Smyth",
        "dateOfBirth": "1978-05-11",
        "preferredName": None,
        "occupation": "sales_Marketing",
        "occupationIndustry": "retail",
        "allowAgencyParticipation": True,
    }
    humans.extend([hitl_a, hitl_b])

    return entities, humans


def _shared_rel_signal(
    rels: list[dict], name_a: str, name_b: str
) -> tuple[bool, list[str]]:
    a = _normalize_name(name_a)
    b = _normalize_name(name_b)
    a_edges = {
        (r["rel_type"], _normalize_name(r["dst_name"])) for r in rels if _normalize_name(r["src_name"]) == a
    }
    b_edges = {
        (r["rel_type"], _normalize_name(r["dst_name"])) for r in rels if _normalize_name(r["src_name"]) == b
    }
    common = a_edges & b_edges
    if common:
        return True, [f"{k[0]} -> {k[1]}" for k in list(common)[:3]]
    return False, []


def cluster_entities(entities: list[dict], rels: list[dict]) -> list[Cluster]:
    seen: set[str] = set()
    clusters: list[Cluster] = []
    if not entities:
        return clusters
    texts = [_entity_blob(e) for e in entities]
    try:
        vec = TfidfVectorizer(analyzer="word", ngram_range=(1, 2), min_df=1)
        mat = vec.fit_transform(texts)
        sim = cosine_similarity(mat)
    except ValueError:
        sim = None

    for i, a in enumerate(entities):
        if a["entityIdentifier"] in seen:
            continue
        members = [a]
        signals_log: list[str] = []
        conf = 0.0
        for j, b in enumerate(entities):
            if i == j or b["entityIdentifier"] in seen:
                continue
            sigs = []
            fein_match = bool(a.get("fein") and b.get("fein") and a["fein"] == b["fein"])
            if fein_match:
                sigs.append("FEIN exact match")
            name_ratio = fuzz.ratio(_normalize_name(a.get("name")), _normalize_name(b.get("name")))
            if name_ratio >= 85:
                sigs.append(f"Name ratio {name_ratio}")
            tfidf = float(sim[i][j]) if sim is not None else 0.0
            if tfidf >= 0.60:
                sigs.append(f"TF-IDF {tfidf:.2f}")
            shared, edges = _shared_rel_signal(rels, a.get("name") or "", b.get("name") or "")
            if shared:
                sigs.append("Shared relationships: " + ", ".join(edges))
            if not sigs:
                continue
            score = 0.0
            if fein_match and name_ratio >= 85:
                score = 0.96
            elif fein_match:
                score = 0.82 + 0.10 * tfidf
            else:
                score = 0.25 * (name_ratio / 100) + 0.30 * tfidf
                if shared:
                    score += 0.10
            if len(sigs) >= 3 and not fein_match:
                score += 0.04
            score = min(0.99, round(score, 2))
            if score >= 0.70:
                members.append(b)
                signals_log.extend(sigs)
                conf = max(conf, score)
                seen.add(b["entityIdentifier"])
        if len(members) > 1:
            seen.add(a["entityIdentifier"])
            clusters.append(
                Cluster(
                    kind="entity",
                    winner_ref=a["entityIdentifier"],
                    members=members,
                    signals=signals_log,
                    confidence=conf,
                    auto_merged=conf >= 0.85,
                    status="auto" if conf >= 0.85 else "hitl",
                )
            )
    return clusters


def cluster_humans(humans: list[dict], rels: list[dict]) -> list[Cluster]:
    seen: set[str] = set()
    clusters: list[Cluster] = []
    if not humans:
        return clusters
    texts = [_human_blob(h) for h in humans]
    try:
        vec = TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 4), min_df=1)
        mat = vec.fit_transform(texts)
        sim = cosine_similarity(mat)
    except ValueError:
        sim = None

    def canonical_full_name(h):
        return f"{h.get('firstName','')} {h.get('lastName','')}".strip()

    for i, a in enumerate(humans):
        if a["humanIdentifier"] in seen:
            continue
        members = [a]
        signals_log: list[str] = []
        conf = 0.0
        for j, b in enumerate(humans):
            if i == j or b["humanIdentifier"] in seen:
                continue
            sigs = []
            ssn_match = bool(a.get("ssn") and b.get("ssn") and a["ssn"] == b["ssn"])
            if ssn_match:
                sigs.append("SSN exact match")
            last_exact = (
                a.get("lastName")
                and b.get("lastName")
                and _normalize_name(a["lastName"]) == _normalize_name(b["lastName"])
            )
            dob_match = (
                a.get("dateOfBirth")
                and b.get("dateOfBirth")
                and a["dateOfBirth"] == b["dateOfBirth"]
            )
            last_dob = bool(last_exact and dob_match)
            if last_dob:
                sigs.append("lastName + DOB match")
            last_ratio = fuzz.ratio(_normalize_name(a.get("lastName")), _normalize_name(b.get("lastName")))
            first_ratio = fuzz.ratio(_normalize_name(a.get("firstName")), _normalize_name(b.get("firstName")))
            pref_match = False
            if a.get("preferredName") and b.get("firstName") and _normalize_name(a["preferredName"]) == _normalize_name(b["firstName"]):
                pref_match = True
            if b.get("preferredName") and a.get("firstName") and _normalize_name(b["preferredName"]) == _normalize_name(a["firstName"]):
                pref_match = True
            name_variant = last_ratio >= 85 and (first_ratio >= 80 or pref_match)
            if name_variant:
                sigs.append(
                    f"Name match (first {first_ratio}, last {last_ratio}{', preferred alias' if pref_match else ''})"
                )
            tfidf = float(sim[i][j]) if sim is not None else 0.0
            if tfidf >= 0.50:
                sigs.append(f"TF-IDF {tfidf:.2f}")
            shared, edges = _shared_rel_signal(rels, canonical_full_name(a), canonical_full_name(b))
            if not shared and (a.get("preferredName") or b.get("preferredName")):
                alt_a = f"{a.get('preferredName') or ''} {a.get('lastName','')}".strip()
                alt_b = f"{b.get('preferredName') or ''} {b.get('lastName','')}".strip()
                shared, edges = _shared_rel_signal(rels, alt_a or canonical_full_name(a), alt_b or canonical_full_name(b))
                if not shared:
                    shared, edges = _shared_rel_signal(rels, canonical_full_name(a), alt_b or canonical_full_name(b))
            if shared:
                sigs.append("Shared employer/relationships: " + ", ".join(edges))
            # Weak-match HITL path: very different DOB but same last-name family + industry
            weak_match = (
                not last_dob
                and not ssn_match
                and not name_variant
                and last_ratio >= 70
                and first_ratio >= 70
                and a.get("occupation")
                and b.get("occupation")
                and a.get("occupation") == b.get("occupation")
            )
            if weak_match:
                sigs.append(f"Weak name + same industry (last {last_ratio}, first {first_ratio})")
            if not sigs:
                continue

            score = 0.0
            if ssn_match:
                score = 0.96
            elif last_dob and name_variant:
                score = 0.92
            elif last_dob and (pref_match or first_ratio >= 60):
                score = 0.90
            elif last_dob:
                score = 0.72
            elif name_variant and shared:
                score = 0.82
            elif name_variant:
                score = 0.70
            elif weak_match:
                score = 0.67
            else:
                score = 0.20 * (last_ratio / 100) + 0.15 * tfidf + (0.10 if shared else 0)

            score = min(0.99, round(score, 2))

            threshold = 0.60
            if score >= threshold:
                members.append(b)
                signals_log.extend(sigs)
                conf = max(conf, score)
                seen.add(b["humanIdentifier"])
        if len(members) > 1:
            seen.add(a["humanIdentifier"])
            clusters.append(
                Cluster(
                    kind="human",
                    winner_ref=a["humanIdentifier"],
                    members=members,
                    signals=signals_log,
                    confidence=conf,
                    auto_merged=conf >= 0.85,
                    status="auto" if conf >= 0.85 else "hitl",
                )
            )
    return clusters
