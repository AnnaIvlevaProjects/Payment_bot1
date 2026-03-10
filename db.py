from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
from typing import Optional

import aiosqlite

PAYMENT_COLUMNS = [f"payment_{idx}" for idx in range(1, 7)]


@dataclass(slots=True)
class User:
    user_id: int
    user_name: Optional[str]
    user_fn: Optional[str]
    user_ln: Optional[str]
    user_email: Optional[str]
    source: Optional[str]
    course_start_date: str
    removed_from_chat: int
    selected_month: Optional[int]
    last_reminder_month: Optional[int]
    last_removal_month: Optional[int]
    payments: dict[str, str]


class Database:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path

    async def init(self) -> None:
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id INTEGER PRIMARY KEY,
                    user_name TEXT,
                    user_FN TEXT,
                    user_LN TEXT,
                    user_email TEXT,
                    source TEXT,
                    payment_1 TEXT DEFAULT 'нет',
                    payment_2 TEXT DEFAULT 'нет',
                    payment_3 TEXT DEFAULT 'нет',
                    payment_4 TEXT DEFAULT 'нет',
                    payment_5 TEXT DEFAULT 'нет',
                    payment_6 TEXT DEFAULT 'нет',
                    course_start_date TEXT,
                    selected_month INTEGER,
                    removed_from_chat INTEGER DEFAULT 0,
                    last_reminder_month INTEGER,
                    last_removal_month INTEGER,
                    created_at TEXT,
                    updated_at TEXT
                )
                """
            )
            await self._ensure_column(conn, "last_reminder_month", "INTEGER")
            await self._ensure_column(conn, "last_removal_month", "INTEGER")
            await conn.commit()

    async def _ensure_column(self, conn: aiosqlite.Connection, column_name: str, definition: str) -> None:
        cursor = await conn.execute("PRAGMA table_info(users)")
        rows = await cursor.fetchall()
        columns = {row[1] for row in rows}
        if column_name not in columns:
            await conn.execute(f"ALTER TABLE users ADD COLUMN {column_name} {definition}")

    async def upsert_user(
        self,
        user_id: int,
        user_name: Optional[str],
        user_fn: Optional[str],
        user_ln: Optional[str],
        source: Optional[str],
        course_start_date: str,
    ) -> None:
        now = datetime.utcnow().isoformat()
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                """
                INSERT INTO users (
                    user_id, user_name, user_FN, user_LN, source, course_start_date, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    user_name = excluded.user_name,
                    user_FN = excluded.user_FN,
                    user_LN = excluded.user_LN,
                    source = COALESCE(users.source, excluded.source),
                    updated_at = excluded.updated_at
                """,
                (user_id, user_name, user_fn, user_ln, source, course_start_date, now, now),
            )
            await conn.commit()

    async def set_selected_month(self, user_id: int, month: int) -> None:
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                "UPDATE users SET selected_month = ?, updated_at = ? WHERE user_id = ?",
                (month, datetime.utcnow().isoformat(), user_id),
            )
            await conn.commit()

    async def clear_selected_month(self, user_id: int) -> None:
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                "UPDATE users SET selected_month = NULL, updated_at = ? WHERE user_id = ?",
                (datetime.utcnow().isoformat(), user_id),
            )
            await conn.commit()

    async def set_email(self, user_id: int, email: str) -> None:
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                "UPDATE users SET user_email = ?, updated_at = ? WHERE user_id = ?",
                (email, datetime.utcnow().isoformat(), user_id),
            )
            await conn.commit()

    async def mark_payment(self, user_id: int, month: int) -> None:
        if month not in range(1, 7):
            raise ValueError("month must be in range 1..6")
        payment_column = f"payment_{month}"
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                f"UPDATE users SET {payment_column} = 'да', selected_month = NULL, removed_from_chat = 0, updated_at = ? WHERE user_id = ?",
                (datetime.utcnow().isoformat(), user_id),
            )
            await conn.commit()

    async def mark_full_payment(self, user_id: int) -> None:
        now = datetime.utcnow().isoformat()
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                """
                UPDATE users
                SET payment_1 = 'да', payment_2 = 'да', payment_3 = 'да',
                    payment_4 = 'да', payment_5 = 'да', payment_6 = 'да',
                    selected_month = NULL, removed_from_chat = 0, updated_at = ?
                WHERE user_id = ?
                """,
                (now, user_id),
            )
            await conn.commit()

    async def get_user(self, user_id: int) -> Optional[User]:
        async with aiosqlite.connect(self.db_path) as conn:
            conn.row_factory = aiosqlite.Row
            cursor = await conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
            row = await cursor.fetchone()
            if not row:
                return None
            payments = {column: row[column] for column in PAYMENT_COLUMNS}
            return User(
                user_id=row["user_id"],
                user_name=row["user_name"],
                user_fn=row["user_FN"],
                user_ln=row["user_LN"],
                user_email=row["user_email"],
                source=row["source"],
                course_start_date=row["course_start_date"],
                removed_from_chat=row["removed_from_chat"],
                selected_month=row["selected_month"],
                last_reminder_month=row["last_reminder_month"],
                last_removal_month=row["last_removal_month"],
                payments=payments,
            )

    async def iter_users(self) -> list[User]:
        async with aiosqlite.connect(self.db_path) as conn:
            conn.row_factory = aiosqlite.Row
            cursor = await conn.execute("SELECT * FROM users")
            rows = await cursor.fetchall()
        users: list[User] = []
        for row in rows:
            users.append(
                User(
                    user_id=row["user_id"],
                    user_name=row["user_name"],
                    user_fn=row["user_FN"],
                    user_ln=row["user_LN"],
                    user_email=row["user_email"],
                    source=row["source"],
                    course_start_date=row["course_start_date"],
                    removed_from_chat=row["removed_from_chat"],
                    selected_month=row["selected_month"],
                    last_reminder_month=row["last_reminder_month"],
                    last_removal_month=row["last_removal_month"],
                    payments={column: row[column] for column in PAYMENT_COLUMNS},
                )
            )
        return users

    async def export_users_table(self) -> tuple[list[str], list[list[str]]]:
        columns = [
            "user_id",
            "user_name",
            "user_FN",
            "user_LN",
            "user_email",
            "source",
            *PAYMENT_COLUMNS,
            "course_start_date",
            "selected_month",
            "removed_from_chat",
            "last_reminder_month",
            "last_removal_month",
            "created_at",
            "updated_at",
        ]
        async with aiosqlite.connect(self.db_path) as conn:
            conn.row_factory = aiosqlite.Row
            cursor = await conn.execute(f"SELECT {', '.join(columns)} FROM users ORDER BY user_id")
            rows = await cursor.fetchall()

        export_rows: list[list[str]] = []
        for row in rows:
            export_rows.append(["" if row[column] is None else str(row[column]) for column in columns])
        return columns, export_rows

    async def set_removed_flag(self, user_id: int, removed: bool) -> None:
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                "UPDATE users SET removed_from_chat = ?, updated_at = ? WHERE user_id = ?",
                (1 if removed else 0, datetime.utcnow().isoformat(), user_id),
            )
            await conn.commit()

    async def set_last_reminder_month(self, user_id: int, month_index: int) -> None:
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                "UPDATE users SET last_reminder_month = ?, updated_at = ? WHERE user_id = ?",
                (month_index, datetime.utcnow().isoformat(), user_id),
            )
            await conn.commit()

    async def set_last_removal_month(self, user_id: int, month_index: int) -> None:
        async with aiosqlite.connect(self.db_path) as conn:
            await conn.execute(
                "UPDATE users SET last_removal_month = ?, updated_at = ? WHERE user_id = ?",
                (month_index, datetime.utcnow().isoformat(), user_id),
            )
            await conn.commit()
