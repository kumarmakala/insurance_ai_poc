from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable

from rapidfuzz import distance as rf_distance
from rapidfuzz import fuzz


ENUM_FIELDS = {
    "legalEntityType": "Legal Entity Type",
    "prefix": "Prefix",
    "pronoun": "Pronoun",
    "educationLevel": "Education Level",
    "occupation": "Occupation",
    "occupationIndustry": "Occupation Industry",
    "gender": "Gender",
    "maritalStatus": "Marital Status",
    "role": "Entity To Human Roles",
}


ENTITY_CANONICAL = {
    "entityIdentifier",
    "name",
    "legalEntityType",
    "dateOfIncorporation",
    "entityDescription",
    "annualRevenue",
    "isClosed",
    "doingBusinessAs",
    "fein",
    "ssn",
    "allowAgencyParticipation",
}

HUMAN_CANONICAL = {
    "humanIdentifier",
    "prefix",
    "firstName",
    "middleName",
    "lastName",
    "preferredName",
    "pronoun",
    "dateOfBirth",
    "educationLevel",
    "occupation",
    "occupationIndustry",
    "yearOccupationStated",
    "isDeceased",
    "gender",
    "maritalStatus",
    "ssn",
    "licenseNumber",
    "licenseState",
    "firstLicensedDate",
    "allowAgencyParticipation",
}

CONTACT_CANONICAL = {
    "parentModuleType",
    "parentModuleIdentifier",
    "contactType",
    "entityIdentifier",
    "role",
    "title",
    "name",
    "preferredContactMethod",
    "marketing",
    "physicalAddress_line1",
    "physicalAddress_city",
    "physicalAddress_state",
    "physicalAddress_country",
    "mailingAddress_line1",
    "mailingAddress_city",
    "mailingAddress_state",
    "primaryPhone_number",
    "primaryPhone_countryCode",
    "emailAddress",
}

CANONICAL_BY_DOMAIN = {
    "entity": ENTITY_CANONICAL,
    "human": HUMAN_CANONICAL,
    "contact": CONTACT_CANONICAL,
}


@dataclass
class MappingRow:
    source_field: str
    canonical_field: str | None
    sample_value: str | None
    status: str  # ok | warn | error
    note: str
    suggested_fix: str | None = None
    confidence: float = 1.0
    record_ref: str | None = None


def _match_column(src: str, targets: set[str]) -> tuple[str | None, float]:
    if not src:
        return None, 0.0
    if src in targets:
        return src, 1.0
    low = src.lower()
    for t in targets:
        if t.lower() == low:
            return t, 0.98
    best, best_score = None, 0
    for t in targets:
        s = fuzz.ratio(src.lower(), t.lower())
        if s > best_score:
            best, best_score = t, s
    if best_score >= 85:
        return best, best_score / 100
    return None, best_score / 100


def _suggest_enum(value: str, valid: Iterable[str]) -> tuple[str | None, float, str]:
    best = None
    best_d = 99
    for v in valid:
        d = rf_distance.Levenshtein.distance(value.lower(), v.lower())
        if d < best_d:
            best, best_d = v, d
    if best is None:
        return None, 0.0, ""
    if best_d == 0:
        return best, 1.0, "exact match"
    length = max(1, len(best))
    confidence = max(0.0, 1.0 - (best_d / length))
    confidence = round(min(0.98, confidence + 0.1), 2)
    note = f"{best_d}-char Levenshtein distance from valid enum"
    return best, confidence, note


def map_columns(domain: str, source_fields: list[str]) -> list[MappingRow]:
    canonical = CANONICAL_BY_DOMAIN[domain]
    out: list[MappingRow] = []
    for src in source_fields:
        if not src:
            continue
        target, conf = _match_column(src, canonical)
        if target and conf >= 0.98:
            out.append(MappingRow(src, target, None, "ok", "direct match", confidence=conf))
        elif target:
            out.append(
                MappingRow(src, target, None, "warn", "fuzzy column match", confidence=conf)
            )
        else:
            out.append(
                MappingRow(src, None, None, "warn", "no canonical target", confidence=conf)
            )
    return out


def validate_enum_values(
    records: list[dict], domain: str, reference: dict[str, list[str]]
) -> list[MappingRow]:
    """Scan every enum-typed field against its reference list. Emit one MappingRow per violation."""
    issues: list[MappingRow] = []
    id_key = {"entity": "entityIdentifier", "human": "humanIdentifier"}.get(domain, "entityIdentifier")
    for rec in records:
        for field_name, cat_name in ENUM_FIELDS.items():
            if field_name not in rec:
                continue
            raw = rec.get(field_name)
            if raw is None or raw == "":
                continue
            valid = reference.get(cat_name, [])
            if not valid:
                continue
            val = str(raw).strip()
            if val in valid:
                continue
            suggested, conf, note = _suggest_enum(val, valid)
            if suggested is None:
                continue
            ref = rec.get(id_key) or rec.get("humanIdentifier") or rec.get("entityIdentifier") or "?"
            name = rec.get("firstName") and f"{rec.get('firstName')} {rec.get('lastName')}" or rec.get("name") or ref
            issues.append(
                MappingRow(
                    source_field=field_name,
                    canonical_field=field_name,
                    sample_value=val,
                    status="error" if conf >= 0.8 else "warn",
                    note=f"{note}; no other candidates within {max(1, int(5*(1-conf)))}",
                    suggested_fix=suggested,
                    confidence=conf,
                    record_ref=f"{ref} ({name})",
                )
            )
    return issues
