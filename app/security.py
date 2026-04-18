from __future__ import annotations

PII_FIELDS = {
    "ssn": "highly_sensitive",
    "fein": "sensitive",
    "license_number": "sensitive",
    "licenseNumber": "sensitive",
    "date_of_birth": "sensitive",
    "dateOfBirth": "sensitive",
    "email": "contact",
    "emailAddress": "contact",
    "primary_phone": "contact",
    "primaryPhone_number": "contact",
}


def classify(field: str) -> str | None:
    return PII_FIELDS.get(field)


def mask(field: str, value):
    if value is None or value == "":
        return value
    klass = classify(field)
    if not klass:
        return value
    s = str(value)
    f_low = field.lower()
    if "ssn" in f_low:
        return "***-**-" + s[-4:] if len(s) >= 4 else "***-**-****"
    if "fein" in f_low:
        return "**-*****" + s[-3:] if len(s) >= 3 else "**-*****"
    if "email" in f_low:
        name, _, domain = s.partition("@")
        if not domain:
            return "***"
        return name[:1] + "***@" + domain
    if "phone" in f_low:
        return "***-***-" + s[-4:] if len(s) >= 4 else "***-***-****"
    if "birth" in f_low or field == "dateOfBirth":
        return s[:4] + "-**-**" if len(s) >= 4 else "****"
    if "license" in f_low:
        return s[:2] + "******" if len(s) > 2 else "******"
    return "****"
