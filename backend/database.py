"""
SQLite checkpointer and thread helper functions.
Standalone module — no circular deps.
"""

import sqlite3

from langgraph.checkpoint.sqlite import SqliteSaver

from pdf_ingestion import _THREAD_METADATA, _THREAD_RETRIEVERS

# -------------------
# Checkpointer
# -------------------
conn = sqlite3.connect(database="chatbot.db", check_same_thread=False)
checkpointer = SqliteSaver(conn=conn)

# -------------------
# Message timestamps table
# -------------------
conn.execute("""
    CREATE TABLE IF NOT EXISTS message_timestamps (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        thread_id TEXT NOT NULL,
        role TEXT NOT NULL,
        timestamp TEXT NOT NULL
    )
""")
conn.commit()


# -------------------
# Timestamp helpers
# -------------------
def save_timestamp(thread_id: str, role: str, timestamp: str):
    """Persist a single message timestamp to the database."""
    conn.execute(
        "INSERT INTO message_timestamps (thread_id, role, timestamp) VALUES (?, ?, ?)",
        (thread_id, role, timestamp),
    )
    conn.commit()


def get_timestamps(thread_id: str) -> list[dict]:
    """Return all timestamps for a thread, ordered by insertion order."""
    cursor = conn.execute(
        "SELECT role, timestamp FROM message_timestamps WHERE thread_id = ? ORDER BY id",
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

    # Order threads by their most recent message timestamp (descending)
    placeholders = ",".join("?" for _ in all_threads)
    cursor = conn.execute(
        f"""
        SELECT thread_id, MAX(timestamp) as last_active
        FROM message_timestamps
        WHERE thread_id IN ({placeholders})
        GROUP BY thread_id
        ORDER BY last_active DESC
        """,
        list(all_threads),
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
    conn.execute("DELETE FROM checkpoints WHERE thread_id = ?", (thread_id,))
    conn.execute("DELETE FROM writes WHERE thread_id = ?", (thread_id,))
    conn.execute("DELETE FROM message_timestamps WHERE thread_id = ?", (thread_id,))
    conn.commit()
