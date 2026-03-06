from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any
from urllib import error, request

from app.config import load_environment


@dataclass(slots=True)
class CommentAnalysis:
    sentiment: str
    intent: str
    confidence: float
    summary: str
    source: str


class CommentAnalysisService:
    """Small Gemini-backed analyzer for comment text sentiment/intent."""

    def __init__(self) -> None:
        # Ensure runtime services pick up local .env values consistently.
        # Override here so stale OS-level GEMINI_* vars do not shadow local demo config.
        load_environment(override=True)
        self.api_key = (os.getenv("GEMINI_API_KEY") or "").strip()
        self.model = (os.getenv("GEMINI_COMMENT_MODEL") or "gemini-2.0-flash").strip()
        self.base_url = (os.getenv("GEMINI_BASE_URL") or "https://generativelanguage.googleapis.com/v1beta").rstrip("/")
        self.timeout_seconds = int((os.getenv("GEMINI_COMMENT_TIMEOUT_SECONDS") or "20").strip())
        self._cache: dict[str, CommentAnalysis] = {}

    def analyze(self, comment_text: str) -> CommentAnalysis:
        text = (comment_text or "").strip()
        if not text:
            return CommentAnalysis(
                sentiment="unknown",
                intent="unknown",
                confidence=0.0,
                summary="Empty comment text.",
                source="empty",
            )
        if text in self._cache:
            return self._cache[text]
        if not self.api_key:
            analysis = CommentAnalysis(
                sentiment="unknown",
                intent="unknown",
                confidence=0.0,
                summary="GEMINI_API_KEY missing; comment analysis skipped.",
                source="missing_api_key",
            )
            self._cache[text] = analysis
            return analysis

        prompt = (
            "Classify this LinkedIn comment for GTM signal.\n"
            "Return only JSON with keys: sentiment, intent, confidence, summary.\n"
            "sentiment must be one of: positive, neutral, negative.\n"
            "intent must be one of: high, medium, low.\n"
            "confidence must be number 0..1.\n"
            f"comment: {text}"
        )
        payload = {
            "contents": [
                {
                    "role": "user",
                    "parts": [{"text": prompt}],
                }
            ],
            "generationConfig": {
                "temperature": 0,
                "responseMimeType": "application/json",
            },
        }
        body = json.dumps(payload).encode("utf-8")
        req = request.Request(
            f"{self.base_url}/models/{self.model}:generateContent?key={self.api_key}",
            data=body,
            headers={
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            with request.urlopen(req, timeout=self.timeout_seconds) as resp:
                raw = resp.read().decode("utf-8")
            data = json.loads(raw)
            content = data["candidates"][0]["content"]["parts"][0]["text"]
            parsed = json.loads(content)
            analysis = CommentAnalysis(
                sentiment=self._normalize_sentiment(parsed.get("sentiment")),
                intent=self._normalize_intent(parsed.get("intent")),
                confidence=self._normalize_confidence(parsed.get("confidence")),
                summary=str(parsed.get("summary") or "").strip()[:280],
                source="gemini",
            )
        except (error.URLError, error.HTTPError, KeyError, ValueError, json.JSONDecodeError):
            analysis = CommentAnalysis(
                sentiment="unknown",
                intent="unknown",
                confidence=0.0,
                summary="Gemini analysis call failed; fallback used.",
                source="gemini_error",
            )
        self._cache[text] = analysis
        return analysis

    @staticmethod
    def _normalize_sentiment(value: Any) -> str:
        text = str(value or "").strip().lower()
        return text if text in {"positive", "neutral", "negative"} else "unknown"

    @staticmethod
    def _normalize_intent(value: Any) -> str:
        text = str(value or "").strip().lower()
        return text if text in {"high", "medium", "low"} else "unknown"

    @staticmethod
    def _normalize_confidence(value: Any) -> float:
        try:
            score = float(value)
        except (TypeError, ValueError):
            return 0.0
        return max(0.0, min(1.0, score))
