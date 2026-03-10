"""
scripts/db.py

Shared PostgreSQL connection + bulk upsert helper.
Used by all transformation and analysis scripts.

Requires env var: DATABASE_URL
  Format: postgresql://postgres:[password]@db.[ref].supabase.co:5432/postgres
  Set in .env locally, GitHub Secret in CI.
"""

import os
import psycopg2
import psycopg2.extras
from dotenv import load_dotenv

load_dotenv()


def get_conn():
    """Return a psycopg2 connection using DATABASE_URL env var."""
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL environment variable is not set.")
    return psycopg2.connect(url)


def upsert(conn, table: str, rows: list[dict], conflict_cols: list[str]):
    """
    Bulk upsert rows into table.

    - rows: list of dicts — all must have the same keys
    - conflict_cols: columns that form the UNIQUE constraint (ON CONFLICT target)
    - All non-conflict columns are updated on conflict (DO UPDATE SET ...)

    Uses psycopg2 execute_values for fast bulk insert.
    """
    if not rows:
        return

    cols = list(rows[0].keys())
    update_cols = [c for c in cols if c not in conflict_cols and c != "id"]

    col_str = ", ".join(f'"{c}"' for c in cols)
    placeholder = "(" + ", ".join(["%s"] * len(cols)) + ")"
    conflict_str = ", ".join(f'"{c}"' for c in conflict_cols)

    if update_cols:
        update_str = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
        # Always bump updated_at
        if "updated_at" in cols:
            update_str += ', "updated_at" = NOW()'
        on_conflict = f"ON CONFLICT ({conflict_str}) DO UPDATE SET {update_str}"
    else:
        on_conflict = f"ON CONFLICT ({conflict_str}) DO NOTHING"

    sql = f'INSERT INTO "{table}" ({col_str}) VALUES %s {on_conflict}'
    values = [tuple(row[c] for c in cols) for row in rows]

    with conn.cursor() as cur:
        psycopg2.extras.execute_values(cur, sql, values, page_size=500)
    conn.commit()
    print(f"  ✅ Upserted {len(rows)} rows into {table}")