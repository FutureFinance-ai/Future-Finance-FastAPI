from __future__ import annotations

import json
from datetime import datetime
from typing import Any, Dict, Optional

from openai import OpenAI
from pydantic import ValidationError
from settings.config import settings
from schemas.cleaned_transaction import CleanedTransaction


SYSTEM_PROMPT = (
    "You are a financial transaction parser. "
    "Extract a single transaction in strict JSON with keys: txn_date (YYYY-MM-DD), amount (float), currency, merchant, raw_description, normalized_desc. "
    "If ambiguous, make the safest best guess; currency should be 3-letter ISO when possible."
)


def _client() -> Optional[OpenAI]:
    if not settings.OPENAI_API_KEY:
        return None
    return OpenAI(api_key=settings.OPENAI_API_KEY)


def _build_user_prompt(blob: dict) -> str:
    return (
        "Input JSON (may be messy, OCR'd, or partial):\n"
        f"{json.dumps(blob, ensure_ascii=False)}\n"
        "Return ONLY the JSON object, no extra text."
    )


def _validate_result(data: Dict[str, Any]) -> CleanedTransaction:
    # Convert txn_date variants to ISO
    if "txn_date" in data and isinstance(data["txn_date"], str):
        # Try flexible parsing; fall back to YYYY-MM-DD enforced by pydantic
        try:
            parsed = datetime.fromisoformat(data["txn_date"].replace("Z", "+00:00")).date()
            data["txn_date"] = parsed.isoformat()
        except Exception:
            pass
    if "currency" in data and isinstance(data["currency"], str):
        data["currency"] = data["currency"].upper()[:8]
    return CleanedTransaction.model_validate(data)


def clean_with_llm(blob: dict, retries: int = 2) -> CleanedTransaction:
    """
    Clean a single raw transaction-like blob via LLM with validation and limited retries.
    If OPENAI_API_KEY is not configured, raise RuntimeError (caller can fallback).
    """
    client = _client()
    if client is None:
        raise RuntimeError("OPENAI_API_KEY not configured")

    last_error: Optional[Exception] = None
    prompt = _build_user_prompt(blob)
    for attempt in range(retries + 1):
        try:
            resp = client.chat.completions.create(
                model="gpt-4o-mini",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                temperature=0.0,
                max_tokens=300,
            )
            content = resp.choices[0].message.content or "{}"
            # Extract JSON from content
            start = content.find("{")
            end = content.rfind("}")
            if start == -1 or end == -1:
                raise ValueError("Model did not return JSON")
            data = json.loads(content[start : end + 1])
            return _validate_result(data)
        except (ValidationError, ValueError, json.JSONDecodeError) as e:
            last_error = e
            # Adjust prompt with error feedback
            prompt = prompt + f"\nPrevious output invalid due to: {str(e)}. Return strict JSON with required keys."
            continue
        except Exception as e:
            last_error = e
            break
    raise RuntimeError(f"LLM cleaning failed: {last_error}")


