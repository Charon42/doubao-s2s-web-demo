from __future__ import annotations

import json
import sqlite3
import traceback
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DB_PATH = DATA_DIR / "app.db"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _json_dumps(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"))


def _connect() -> sqlite3.Connection:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=5)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA locking_mode = EXCLUSIVE")
    conn.execute("PRAGMA journal_mode = MEMORY")
    conn.execute("PRAGMA busy_timeout = 5000")
    return conn


def init_db() -> None:
    try:
        with closing(_connect()) as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS conversations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    session_id TEXT,
                    dialog_id TEXT,
                    user_id TEXT,
                    started_at TEXT NOT NULL,
                    ended_at TEXT,
                    city TEXT,
                    latitude REAL,
                    longitude REAL,
                    summary TEXT
                );

                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    conversation_id INTEGER NOT NULL,
                    role TEXT NOT NULL,
                    text TEXT NOT NULL,
                    question_id TEXT,
                    reply_id TEXT,
                    event_id INTEGER,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (conversation_id) REFERENCES conversations(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS user_preferences (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    conversation_id INTEGER NOT NULL,
                    preference_key TEXT NOT NULL,
                    preference_value TEXT NOT NULL,
                    confidence REAL,
                    updated_at TEXT NOT NULL,
                    FOREIGN KEY (conversation_id) REFERENCES conversations(id) ON DELETE CASCADE,
                    UNIQUE (conversation_id, preference_key)
                );

                CREATE TABLE IF NOT EXISTS tool_calls (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    conversation_id INTEGER NOT NULL,
                    tool_name TEXT NOT NULL,
                    request_json TEXT,
                    response_json TEXT,
                    status TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (conversation_id) REFERENCES conversations(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS recommendations (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    conversation_id INTEGER NOT NULL,
                    rec_type TEXT NOT NULL,
                    name TEXT NOT NULL,
                    address TEXT,
                    latitude REAL,
                    longitude REAL,
                    rating REAL,
                    price TEXT,
                    distance REAL,
                    source TEXT,
                    raw_json TEXT,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (conversation_id) REFERENCES conversations(id) ON DELETE CASCADE
                );

                CREATE INDEX IF NOT EXISTS idx_conversations_session_id
                    ON conversations(session_id);
                CREATE INDEX IF NOT EXISTS idx_messages_conversation_id
                    ON messages(conversation_id, created_at);
                CREATE INDEX IF NOT EXISTS idx_tool_calls_conversation_id
                    ON tool_calls(conversation_id, created_at);
                CREATE INDEX IF NOT EXISTS idx_recommendations_conversation_id
                    ON recommendations(conversation_id, created_at);
                """
            )
            conn.commit()
    except Exception:
        traceback.print_exc()


def create_conversation(
    session_id: str | None = None,
    dialog_id: str | None = None,
    user_id: str | None = None,
    city: str | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
) -> int | None:
    try:
        with closing(_connect()) as conn:
            cursor = conn.execute(
                """
                INSERT INTO conversations (
                    session_id, dialog_id, user_id, started_at, city, latitude, longitude
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (session_id, dialog_id, user_id, _utc_now(), city, latitude, longitude),
            )
            conn.commit()
            return int(cursor.lastrowid)
    except Exception:
        traceback.print_exc()
        return None


def end_conversation(conversation_id: int, summary: str | None = None) -> bool:
    try:
        with closing(_connect()) as conn:
            conn.execute(
                """
                UPDATE conversations
                SET ended_at = ?, summary = COALESCE(?, summary)
                WHERE id = ?
                """,
                (_utc_now(), summary, conversation_id),
            )
            conn.commit()
            return True
    except Exception:
        traceback.print_exc()
        return False


def save_message(
    conversation_id: int,
    role: str,
    text: str,
    question_id: str | None = None,
    reply_id: str | None = None,
    event_id: int | None = None,
) -> int | None:
    try:
        with closing(_connect()) as conn:
            cursor = conn.execute(
                """
                INSERT INTO messages (
                    conversation_id, role, text, question_id, reply_id, event_id, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (conversation_id, role, text, question_id, reply_id, event_id, _utc_now()),
            )
            conn.commit()
            return int(cursor.lastrowid)
    except Exception:
        traceback.print_exc()
        return None


def save_tool_call(
    conversation_id: int,
    tool_name: str,
    request_json: Any,
    response_json: Any,
    status: str,
) -> int | None:
    try:
        with closing(_connect()) as conn:
            cursor = conn.execute(
                """
                INSERT INTO tool_calls (
                    conversation_id, tool_name, request_json, response_json, status, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    conversation_id,
                    tool_name,
                    _json_dumps(request_json),
                    _json_dumps(response_json),
                    status,
                    _utc_now(),
                ),
            )
            conn.commit()
            return int(cursor.lastrowid)
    except Exception:
        traceback.print_exc()
        return None


def save_recommendation(
    conversation_id: int,
    rec_type: str,
    name: str,
    address: str | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
    rating: float | None = None,
    price: str | None = None,
    distance: float | None = None,
    source: str | None = None,
    raw_json: Any = None,
) -> int | None:
    try:
        with closing(_connect()) as conn:
            cursor = conn.execute(
                """
                INSERT INTO recommendations (
                    conversation_id, rec_type, name, address, latitude, longitude,
                    rating, price, distance, source, raw_json, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    conversation_id,
                    rec_type,
                    name,
                    address,
                    latitude,
                    longitude,
                    rating,
                    price,
                    distance,
                    source,
                    _json_dumps(raw_json),
                    _utc_now(),
                ),
            )
            conn.commit()
            return int(cursor.lastrowid)
    except Exception:
        traceback.print_exc()
        return None


def update_conversation_location(
    conversation_id: int,
    city: str | None = None,
    latitude: float | None = None,
    longitude: float | None = None,
) -> bool:
    try:
        with closing(_connect()) as conn:
            conn.execute(
                """
                UPDATE conversations
                SET city = COALESCE(?, city),
                    latitude = COALESCE(?, latitude),
                    longitude = COALESCE(?, longitude)
                WHERE id = ?
                """,
                (city, latitude, longitude, conversation_id),
            )
            conn.commit()
            return True
    except Exception:
        traceback.print_exc()
        return False
