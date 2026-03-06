from __future__ import annotations

import re
from urllib.parse import urlsplit, urlunsplit

COMPANY_SUFFIXES = {
    "inc",
    "incorporated",
    "llc",
    "l.l.c",
    "ltd",
    "limited",
    "corp",
    "corporation",
    "co",
    "company",
    "plc",
    "gmbh",
    "sa",
}


def clean_text(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    return text or None


def normalize_whitespace(value: str) -> str:
    return " ".join(value.split())


def normalize_person_name(value: str | None) -> str | None:
    text = clean_text(value)
    if text is None:
        return None
    text = re.sub(r"[^a-zA-Z0-9\s]", " ", text).lower()
    text = normalize_whitespace(text)
    return text or None


def normalize_company_name(value: str | None) -> str | None:
    text = clean_text(value)
    if text is None:
        return None

    text = text.lower().replace("&", " and ")
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    tokens = [token for token in normalize_whitespace(text).split(" ") if token]
    filtered = [token for token in tokens if token not in COMPANY_SUFFIXES]
    normalized = " ".join(filtered)
    return normalized or None


def normalize_linkedin_url(value: str | None) -> str | None:
    text = clean_text(value)
    if text is None:
        return None
    if not text.lower().startswith(("http://", "https://")):
        return None

    split = urlsplit(text)
    host = split.netloc.lower()
    if host.startswith("m.linkedin.com"):
        host = "www.linkedin.com"
    elif host == "linkedin.com":
        host = "www.linkedin.com"
    elif host.startswith("www.linkedin.com"):
        host = "www.linkedin.com"

    if "linkedin.com" not in host:
        return None

    path = split.path.rstrip("/")
    while "//" in path:
        path = path.replace("//", "/")
    if not path:
        return None

    return urlunsplit(("https", host, path, "", ""))


def normalize_domain(value: str | None) -> str | None:
    text = clean_text(value)
    if text is None:
        return None

    lowered = text.lower()
    lowered = lowered.replace("https://", "").replace("http://", "")
    lowered = lowered.split("/")[0]
    lowered = lowered.strip()
    if lowered.startswith("www."):
        lowered = lowered[4:]
    return lowered or None


def looks_like_domain(value: str | None) -> bool:
    domain = normalize_domain(value)
    return bool(domain and "." in domain and " " not in domain)