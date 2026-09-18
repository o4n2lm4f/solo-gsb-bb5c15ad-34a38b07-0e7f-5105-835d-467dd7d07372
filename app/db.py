"""SQLite persistence layer.

A single connection guarded by a lock serializes all access, which keeps the
claim/update sequences atomic for the in-process workers. WAL mode is enabled
so readers are not blocked while a worker writes attempt results.
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
import uuid

SCHEMA = """
CREATE TABLE IF NOT EXISTS subscriptions (
    id          TEXT PRIMARY KEY,
    target_url  TEXT NOT NULL,
    secret      TEXT NOT NULL,
    created_at  REAL NOT NULL,
    paused_at   REAL                   -- epoch seconds while paused; NULL when active
);

CREATE TABLE IF NOT EXISTS events (
    id          TEXT PRIMARY KEY,
    payload     TEXT NOT NULL,          -- raw JSON body delivered to targets
    created_at  REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id         TEXT NOT NULL REFERENCES events(id),
    subscription_id  TEXT NOT NULL,
    status           TEXT NOT NULL DEFAULT 'pending'
                     CHECK (status IN ('pending', 'in_progress', 'delivered', 'failed')),
    attempts         INTEGER NOT NULL DEFAULT 0,
    max_attempts     INTEGER NOT NULL,
    next_attempt_at  REAL,              -- epoch seconds; NULL once finished
    claimed_by       TEXT,
    last_status_code INTEGER,
    last_error       TEXT,
    result           TEXT CHECK (result IN ('delivered', 'failed')),
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL,
    UNIQUE (event_id, subscription_id)  -- one task per (event, subscription)
);

CREATE TABLE IF NOT EXISTS attempts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id     INTEGER NOT NULL REFERENCES tasks(id),
    attempt_no  INTEGER NOT NULL,
    status_code INTEGER,                -- NULL when the request never got a response
    error       TEXT,
    started_at  REAL NOT NULL,
    finished_at REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS ingested_events (
    id          TEXT PRIMARY KEY,       -- X-Webhook-Id of a verified inbound event
    raw_body    BLOB NOT NULL,          -- exact original JSON bytes, for conflict checks
    receipt     TEXT NOT NULL,          -- JSON acceptance response returned first time
    created_at  REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tasks_due ON tasks (status, next_attempt_at);
CREATE INDEX IF NOT EXISTS idx_tasks_sub_order ON tasks (subscription_id, id, status);
CREATE INDEX IF NOT EXISTS idx_attempts_task ON attempts (task_id);
"""


class Database:
    def __init__(self, path: str):
        self._conn = sqlite3.connect(path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._lock = threading.Lock()
        with self._lock:
            self._conn.executescript(SCHEMA)
            self._migrate()
            self._conn.commit()

    def _migrate(self) -> None:
        """Bring databases created by older versions up to date.

        CREATE TABLE IF NOT EXISTS leaves pre-existing tables untouched, so
        columns added later are applied here via ALTER TABLE.
        """
        cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(subscriptions)")}
        if "paused_at" not in cols:
            self._conn.execute("ALTER TABLE subscriptions ADD COLUMN paused_at REAL")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    # ------------------------------------------------------------------
    # subscriptions
    # ------------------------------------------------------------------
    def create_subscription(self, target_url: str, secret: str) -> dict:
        sub_id = uuid.uuid4().hex[:12]
        now = time.time()
        with self._lock:
            self._conn.execute(
                "INSERT INTO subscriptions (id, target_url, secret, created_at) VALUES (?, ?, ?, ?)",
                (sub_id, target_url, secret, now),
            )
            self._conn.commit()
        return {"id": sub_id, "target_url": target_url, "created_at": now, "paused_at": None}

    def list_subscriptions(self) -> list[dict]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, target_url, created_at, paused_at FROM subscriptions ORDER BY created_at"
            ).fetchall()
        return [dict(r) for r in rows]

    def get_subscription(self, sub_id: str) -> dict | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT id, target_url, created_at, paused_at FROM subscriptions WHERE id = ?",
                (sub_id,),
            ).fetchone()
        return dict(row) if row else None

    def set_subscription_paused(self, sub_id: str, paused: bool) -> dict | None:
        """Pause or resume a subscription, idempotently.

        Only the paused_at flag on the subscription row is flipped; task
        rows are never touched, so every pending task keeps its original
        next_attempt_at schedule and simply becomes claimable again once
        the subscription is resumed. A repeated pause keeps the original
        paused_at timestamp. Returns the updated row, or None when the
        subscription does not exist.
        """
        with self._lock:
            row = self._conn.execute(
                "SELECT paused_at FROM subscriptions WHERE id = ?", (sub_id,)
            ).fetchone()
            if row is None:
                return None
            if paused and row["paused_at"] is None:
                self._conn.execute(
                    "UPDATE subscriptions SET paused_at = ? WHERE id = ?",
                    (time.time(), sub_id),
                )
            elif not paused and row["paused_at"] is not None:
                self._conn.execute(
                    "UPDATE subscriptions SET paused_at = NULL WHERE id = ?", (sub_id,)
                )
            self._conn.commit()
            row = self._conn.execute(
                "SELECT id, target_url, created_at, paused_at FROM subscriptions WHERE id = ?",
                (sub_id,),
            ).fetchone()
        return dict(row)

    def subscription_exists(self, sub_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM subscriptions WHERE id = ?", (sub_id,)
            ).fetchone()
        return row is not None

    def delete_subscription(self, sub_id: str) -> bool:
        with self._lock:
            cur = self._conn.execute("DELETE FROM subscriptions WHERE id = ?", (sub_id,))
            self._conn.commit()
        return cur.rowcount > 0

    # ------------------------------------------------------------------
    # events and tasks
    # ------------------------------------------------------------------
    def get_event_payload(self, event_id: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT payload FROM events WHERE id = ?", (event_id,)
            ).fetchone()
        return row["payload"] if row else None

    def create_event(self, event_id: str, payload: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT INTO events (id, payload, created_at) VALUES (?, ?, ?)",
                (event_id, payload, time.time()),
            )
            self._conn.commit()

    def create_task(self, event_id: str, subscription_id: str, max_attempts: int) -> tuple[dict, bool]:
        """Insert a task unless one already exists for (event_id, subscription_id).

        Returns (task_row, created) so callers can report duplicates.
        """
        now = time.time()
        with self._lock:
            cur = self._conn.execute(
                """INSERT OR IGNORE INTO tasks
                       (event_id, subscription_id, status, attempts, max_attempts,
                        next_attempt_at, created_at, updated_at)
                   VALUES (?, ?, 'pending', 0, ?, ?, ?, ?)""",
                (event_id, subscription_id, max_attempts, now, now, now),
            )
            created = cur.rowcount > 0
            row = self._conn.execute(
                "SELECT * FROM tasks WHERE event_id = ? AND subscription_id = ?",
                (event_id, subscription_id),
            ).fetchone()
            self._conn.commit()
        return dict(row), created

    def ingest_event(
        self,
        webhook_id: str,
        raw_body: bytes,
        subscription_ids: list[str],
        max_attempts: int,
    ) -> tuple[str, dict | list[str]]:
        """Atomically record a verified inbound event and enqueue its tasks.

        Runs in a single transaction so a concurrent replay with the same
        X-Webhook-Id can never slip past the idempotency check.

        Returns one of:
            ("duplicate", receipt_dict)  -- id already accepted, body identical;
                                            the original acceptance is returned
            ("conflict", None)           -- id already exists with a different body
            ("unknown", [ids])           -- unknown subscription id(s); nothing stored
            ("created", receipt_dict)    -- first acceptance; tasks were created
        """
        now = time.time()
        with self._lock:
            row = self._conn.execute(
                "SELECT raw_body, receipt FROM ingested_events WHERE id = ?",
                (webhook_id,),
            ).fetchone()
            if row is not None:
                if row["raw_body"] == raw_body:
                    return "duplicate", json.loads(row["receipt"])
                return "conflict", None

            # An event created via POST /events shares the events namespace.
            # Treat an incompatible row as a conflict as well.
            existing = self._conn.execute(
                "SELECT payload FROM events WHERE id = ?", (webhook_id,)
            ).fetchone()
            if existing is not None and existing["payload"].encode("utf-8") != raw_body:
                return "conflict", None

            unknown = [
                sid
                for sid in dict.fromkeys(subscription_ids)
                if self._conn.execute(
                    "SELECT 1 FROM subscriptions WHERE id = ?", (sid,)
                ).fetchone()
                is None
            ]
            if unknown:
                return "unknown", unknown

            if existing is None:
                self._conn.execute(
                    "INSERT INTO events (id, payload, created_at) VALUES (?, ?, ?)",
                    (webhook_id, raw_body.decode("utf-8"), now),
                )

            created, duplicates = [], []
            for sid in dict.fromkeys(subscription_ids):  # preserve order, drop repeats
                cur = self._conn.execute(
                    """INSERT OR IGNORE INTO tasks
                           (event_id, subscription_id, status, attempts, max_attempts,
                            next_attempt_at, created_at, updated_at)
                       VALUES (?, ?, 'pending', 0, ?, ?, ?, ?)""",
                    (webhook_id, sid, max_attempts, now, now, now),
                )
                task = self._conn.execute(
                    "SELECT id, status FROM tasks WHERE event_id = ? AND subscription_id = ?",
                    (webhook_id, sid),
                ).fetchone()
                ref = {"task_id": task["id"], "subscription_id": sid, "status": task["status"]}
                (created if cur.rowcount > 0 else duplicates).append(ref)

            receipt = {"event_id": webhook_id, "created": created, "duplicates": duplicates}
            self._conn.execute(
                "INSERT INTO ingested_events (id, raw_body, receipt, created_at) VALUES (?, ?, ?, ?)",
                (webhook_id, raw_body, json.dumps(receipt), now),
            )
            self._conn.commit()
        return "created", receipt

    def claim_due_task(self, worker_id: str, now: float) -> dict | None:
        """Atomically claim the oldest due task for this worker.

        The SELECT-then-UPDATE runs under the connection lock, so two workers
        can never claim the same task row.

        Strict per-subscription ordering: a task is eligible only when no
        earlier task (smaller id) of the same subscription is still
        'pending' or 'in_progress' -- the next task is released once its
        predecessor reaches a terminal state ('delivered'/'failed'). The
        rule is evaluated against persisted state at claim time, so it
        holds across retry backoff, concurrent workers, and restarts.
        Different subscriptions are independent and still claimed in
        parallel; a blocked task simply stays 'pending' and remains
        visible as such in status queries.

        Paused subscriptions (paused_at set) are skipped entirely: their
        tasks stay 'pending' with their original next_attempt_at and
        become claimable again, in the same order, once resumed. Tasks
        already 'in_progress' when the pause lands are unaffected and
        run to completion.
        """
        with self._lock:
            row = self._conn.execute(
                """SELECT t.*, s.target_url, s.secret, e.payload
                   FROM tasks t
                   LEFT JOIN subscriptions s ON s.id = t.subscription_id
                   LEFT JOIN events e ON e.id = t.event_id
                   WHERE t.status = 'pending' AND t.next_attempt_at <= ?
                     AND s.paused_at IS NULL
                     AND NOT EXISTS (
                           SELECT 1 FROM tasks earlier
                           WHERE earlier.subscription_id = t.subscription_id
                             AND earlier.id < t.id
                             AND earlier.status IN ('pending', 'in_progress')
                         )
                   ORDER BY t.next_attempt_at, t.id
                   LIMIT 1""",
                (now,),
            ).fetchone()
            if row is None:
                return None
            self._conn.execute(
                "UPDATE tasks SET status = 'in_progress', claimed_by = ?, updated_at = ? WHERE id = ?",
                (worker_id, now, row["id"]),
            )
            self._conn.commit()
        return dict(row)

    def finish_attempt(
        self,
        task_id: int,
        attempt_no: int,
        status_code: int | None,
        error: str | None,
        started_at: float,
        finished_at: float,
        outcome: str,  # 'delivered' | 'failed' | 'retry'
        next_attempt_at: float | None,
    ) -> None:
        """Record an attempt and move the task to its next state, in one commit."""
        with self._lock:
            self._conn.execute(
                """INSERT INTO attempts (task_id, attempt_no, status_code, error, started_at, finished_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (task_id, attempt_no, status_code, error, started_at, finished_at),
            )
            if outcome == "retry":
                self._conn.execute(
                    """UPDATE tasks SET status = 'pending', attempts = ?, next_attempt_at = ?,
                           last_status_code = ?, last_error = ?, claimed_by = NULL, updated_at = ?
                       WHERE id = ?""",
                    (attempt_no, next_attempt_at, status_code, error, finished_at, task_id),
                )
            else:
                self._conn.execute(
                    """UPDATE tasks SET status = ?, result = ?, attempts = ?, next_attempt_at = NULL,
                           last_status_code = ?, last_error = ?, claimed_by = NULL, updated_at = ?
                       WHERE id = ?""",
                    (outcome, outcome, attempt_no, status_code, error, finished_at, task_id),
                )
            self._conn.commit()

    def requeue_in_progress(self) -> int:
        """Reset tasks left 'in_progress' by a previous run (crash/restart recovery)."""
        now = time.time()
        with self._lock:
            cur = self._conn.execute(
                """UPDATE tasks SET status = 'pending', next_attempt_at = ?, claimed_by = NULL, updated_at = ?
                   WHERE status = 'in_progress'""",
                (now, now),
            )
            self._conn.commit()
        return cur.rowcount

    def event_status(self, event_id: str) -> dict | None:
        """Full status of an event: its tasks plus per-task attempt history."""
        with self._lock:
            event = self._conn.execute(
                "SELECT id, created_at FROM events WHERE id = ?", (event_id,)
            ).fetchone()
            if event is None:
                return None
            tasks = self._conn.execute(
                """SELECT t.*, s.target_url
                   FROM tasks t LEFT JOIN subscriptions s ON s.id = t.subscription_id
                   WHERE t.event_id = ? ORDER BY t.id""",
                (event_id,),
            ).fetchall()
            attempts = self._conn.execute(
                """SELECT a.* FROM attempts a
                   JOIN tasks t ON t.id = a.task_id
                   WHERE t.event_id = ? ORDER BY a.task_id, a.attempt_no""",
                (event_id,),
            ).fetchall()

        history: dict[int, list[dict]] = {}
        for a in attempts:
            history.setdefault(a["task_id"], []).append(dict(a))
        return {
            "event": dict(event),
            "tasks": [dict(t) for t in tasks],
            "attempts_by_task": history,
        }
