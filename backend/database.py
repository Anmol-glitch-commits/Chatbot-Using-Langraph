"""
PostgreSQL checkpointer and thread helper functions.
Standalone module — no circular deps.
"""

import os

from psycopg_pool import ConnectionPool
from langgraph.checkpoint.postgres import PostgresSaver
from dotenv import load_dotenv

from pdf_ingestion import _THREAD_METADATA, _THREAD_RETRIEVERS

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is required")

# -------------------
# Connection pool (each request gets its own connection — no prepared-statement conflicts)
# -------------------
pool = ConnectionPool(
    DATABASE_URL, min_size=2, max_size=10, kwargs={"autocommit": True}
)

# -------------------
# Checkpointer
# -------------------
checkpointer = PostgresSaver(pool)
checkpointer.setup()

# -------------------
# Message timestamps table
# -------------------
with pool.connection() as conn:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS message_timestamps (
            id SERIAL PRIMARY KEY,
            thread_id TEXT NOT NULL,
            role TEXT NOT NULL,
            timestamp TEXT NOT NULL
        )
    """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS users(
            id SERIAL PRIMARY KEY,
            email TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS chat_threads(
            thread_id TEXT PRIMARY KEY,
            user_id INTEGER NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY(user_id) references users(id)
        )
    """
    )


# -------------------
# Timestamp helpers
# -------------------
def save_timestamp(thread_id: str, role: str, timestamp: str):
    """Persist a single message timestamp to the database."""
    with pool.connection() as conn:
        conn.execute(
            "INSERT INTO message_timestamps (thread_id, role, timestamp) VALUES (%s, %s, %s)",
            (thread_id, role, timestamp),
        )


def get_timestamps(thread_id: str) -> list[dict]:
    """Return all timestamps for a thread, ordered by insertion order."""
    with pool.connection() as conn:
        cursor = conn.execute(
            "SELECT role, timestamp FROM message_timestamps WHERE thread_id = %s ORDER BY id",
            (thread_id,),
        )
        return [{"role": row[0], "timestamp": row[1]} for row in cursor.fetchall()]


# -------------------
# Thread helpers
# -------------------
def retrieve_all_threads():
    """Return all thread IDs, ordered by most recent activity (latest first)."""
    all_threads = set()
    for checkpoint in checkpointer.list(None):
        all_threads.add(checkpoint.config["configurable"]["thread_id"])

    if not all_threads:
        return []

    with pool.connection() as conn:
        cursor = conn.execute(
            """
            SELECT thread_id, MAX(timestamp) as last_active
            FROM message_timestamps
            WHERE thread_id = ANY(%s)
            GROUP BY thread_id
            ORDER BY last_active DESC
            """,
            (list(all_threads),),
        )

        ordered_threads = []
        seen = set()
        for row in cursor.fetchall():
            ordered_threads.append(row[0])
            seen.add(row[0])

    # Threads without any timestamps go at the end
    for thread_id in all_threads:
        if thread_id not in seen:
            ordered_threads.append(thread_id)

    return ordered_threads


def thread_has_document(thread_id: str) -> bool:
    return str(thread_id) in _THREAD_RETRIEVERS


def thread_document_metadata(thread_id: str) -> dict:
    return _THREAD_METADATA.get(str(thread_id), {})


def delete_thread(thread_id: str):
    with pool.connection() as conn:
        conn.execute("DELETE FROM checkpoints WHERE thread_id = %s", (thread_id,))
        conn.execute("DELETE FROM checkpoint_writes WHERE thread_id = %s", (thread_id,))
        conn.execute("DELETE FROM checkpoint_blobs WHERE thread_id = %s", (thread_id,))
        conn.execute(
            "DELETE FROM message_timestamps WHERE thread_id = %s", (thread_id,)
        )


def create_user(email: str, password_hash: str):
    with pool.connection() as conn:
        row = conn.execute(
            """INSERT INTO users (email, password_hash) VALUES (%s, %s) RETURNING id,email,created_at""",
            (email, password_hash),
        ).fetchone()

        return {"id": row[0], "email": row[1], "created_at": row[2]}


def get_all_users():
    with pool.connection() as conn:
        rows = conn.execute(
            "SELECT id, email,password_hash, created_at FROM users ORDER BY id"
        ).fetchall()

    return [
        {
            "id": row[0],
            "email": row[1],
            "password_hash": row[2],
            "created_at": row[3],
        }
        for row in rows
    ]
