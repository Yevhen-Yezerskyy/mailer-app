# FILE: engine/common/cache/__init__.py  (новое — 2025-12-20)
# Смысл: публичный API дев-кеша (IPC через UNIX-socket): memo(query, fn, ttl, version, update).

from .client import memo, CacheClient, DEFAULT_TTL_SEC, DEFAULT_VERSION