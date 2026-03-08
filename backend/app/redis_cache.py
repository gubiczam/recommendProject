from __future__ import annotations

import json
from typing import Any

import redis


class RecommendationCache:
    def __init__(
        self,
        host: str,
        port: int,
        db: int,
        key_prefix: str = "recs:",
        active_version_key: str = "recs:active_version",
    ) -> None:
        self._redis = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        self._key_prefix = key_prefix
        self._active_version_key = active_version_key

    def close(self) -> None:
        self._redis.close()

    def get_recommendations(self, user_id: str) -> list[dict[str, Any]]:
        active_version = self._redis.get(self._active_version_key)
        if active_version:
            raw = self._redis.get(f"{self._key_prefix}{active_version}:{user_id}")
        else:
            raw = None

        if not raw:
            raw = self._redis.get(f"{self._key_prefix}{user_id}")

        if not raw:
            return []
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return []
        if not isinstance(parsed, list):
            return []
        return [item for item in parsed if isinstance(item, dict)]
