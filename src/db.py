import sqlite3


def table_columns(conn: sqlite3.Connection, table: str) -> set[str]:
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table})")
    return {row[1] for row in cur.fetchall()}  # row[1] = column name


def ensure_columns(conn: sqlite3.Connection, table: str, required_cols: dict[str, str]) -> None:
    """
    required_cols = { column_name: sql_type }
    Adds missing columns via ALTER TABLE.
    Safe to call every run.
    """
    existing = table_columns(conn, table)
    cur = conn.cursor()

    for col, col_type in required_cols.items():
        if col not in existing:
            cur.execute(f"ALTER TABLE {table} ADD COLUMN {col} {col_type}")

    conn.commit()


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    )
    return cur.fetchone() is not None
