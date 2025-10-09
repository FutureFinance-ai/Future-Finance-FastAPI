import os
import sys
import json
import uuid
import time
from typing import Tuple

import requests


API_BASE = os.environ.get("API_BASE", "http://localhost:8000")


def print_step(name: str, ok: bool, detail: str = "") -> None:
    status = "OK" if ok else "FAIL"
    line = f"[ {status} ] {name}"
    if detail:
        line += f" -> {detail}"
    print(line)


def ensure_json(resp: requests.Response) -> dict:
    try:
        return resp.json()
    except Exception:
        return {"raw": resp.text}


def check_health() -> Tuple[bool, dict]:
    url = f"{API_BASE}/health"
    r = requests.get(url, timeout=10)
    return r.status_code == 200, ensure_json(r)


def register_user(email: str, password: str) -> Tuple[bool, dict]:
    url = f"{API_BASE}/auth/register"
    payload = {"email": email, "password": password}
    r = requests.post(url, json=payload, timeout=15)
    return r.status_code in (200, 201), ensure_json(r)


def login_user(email: str, password: str) -> Tuple[bool, dict]:
    url = f"{API_BASE}/auth/jwt/login"
    data = {"username": email, "password": password}
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    r = requests.post(url, data=data, headers=headers, timeout=15)
    return r.status_code == 200, ensure_json(r)


def get_me(token: str) -> Tuple[bool, dict]:
    url = f"{API_BASE}/auth/users/me"
    headers = {"Authorization": f"Bearer {token}"}
    r = requests.get(url, headers=headers, timeout=15)
    return r.status_code == 200, ensure_json(r)


def main() -> int:
    email = os.environ.get("SMOKE_EMAIL", f"smoke_{uuid.uuid4().hex[:8]}@example.com")
    password = os.environ.get("SMOKE_PASSWORD", "Secret123!@#")

    ok, data = check_health()
    print_step("GET /health", ok, json.dumps(data))
    if not ok:
        return 1

    ok, data = register_user(email, password)
    print_step("POST /auth/register", ok, json.dumps(data))
    # If already exists, proceed to login anyway

    ok, data = login_user(email, password)
    print_step("POST /auth/jwt/login", ok, json.dumps(data))
    if not ok:
        return 2

    token = data.get("access_token") or data.get("token")
    if not token:
        print_step("extract token", False, json.dumps(data))
        return 3

    ok, data = get_me(token)
    print_step("GET /auth/users/me", ok, json.dumps(data))
    return 0 if ok else 4


if __name__ == "__main__":
    sys.exit(main())


