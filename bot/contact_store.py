from __future__ import annotations

import json
import os
from dataclasses import dataclass, asdict
from typing import Optional

_REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


@dataclass(frozen=True)
class Contact:
    email: str
    phone_e164: str


def _store_path() -> str:
    raw = os.environ.get("CONTACT_STORE_PATH", "").strip()
    if raw:
        return raw if os.path.isabs(raw) else os.path.normpath(os.path.join(_REPO_ROOT, raw))
    os.makedirs(os.path.join(_REPO_ROOT, "data"), exist_ok=True)
    return os.path.join(_REPO_ROOT, "data", "contact.json")


def load_contact() -> Optional[Contact]:
    path = _store_path()
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return None
        email = str(data.get("email") or "").strip()
        phone = str(data.get("phone_e164") or "").strip()
        if not email or not phone:
            return None
        return Contact(email=email, phone_e164=phone)
    except Exception:
        return None


def save_contact(email: str, phone_e164: str) -> Contact:
    c = Contact(email=str(email).strip(), phone_e164=str(phone_e164).strip())
    path = _store_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(asdict(c), f, indent=2)
    os.replace(tmp, path)
    return c

