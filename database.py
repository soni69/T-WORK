"""
База данных для хранения отчётов по продажам.
"""
import aiosqlite
from datetime import datetime, date
from pathlib import Path
from zoneinfo import ZoneInfo
from config import DATABASE_PATH, today_moscow

MOSCOW = ZoneInfo("Europe/Moscow")


async def init_db():
    """Инициализация базы данных."""
    Path(DATABASE_PATH).parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("""
        CREATE TABLE IF NOT EXISTS reports (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            direction TEXT NOT NULL,
            amount REAL NOT NULL,
            comment TEXT,
            report_date DATE NOT NULL,
            report_time TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            user_id INTEGER,
            username TEXT
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS allowed_users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS plans (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            direction TEXT NOT NULL,
            year INTEGER NOT NULL,
            month INTEGER NOT NULL,
            plan REAL NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(direction, year, month)
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS evening_notifications (
            chat_id INTEGER PRIMARY KEY,
            enabled INTEGER NOT NULL DEFAULT 1,
            added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS daily_corrections (
            report_date DATE NOT NULL,
            direction TEXT NOT NULL,
            value REAL NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (report_date, direction)
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS evening_summary_sent (
            report_date DATE PRIMARY KEY
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS pending_access_requests (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            requested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """)
        await db.execute("""
        CREATE TABLE IF NOT EXISTS report_comments (
            report_date DATE NOT NULL,
            report_type TEXT NOT NULL,
            comment TEXT NOT NULL,
            PRIMARY KEY (report_date, report_type)
        )
        """)
        await db.commit()


async def set_report_comment(report_date: date, report_type: str, comment: str) -> None:
    """Сохранить комментарий к дневному или вечернему отчёту за дату."""
    d = report_date.strftime("%Y-%m-%d")
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            """INSERT INTO report_comments (report_date, report_type, comment)
               VALUES (?, ?, ?) ON CONFLICT(report_date, report_type) DO UPDATE SET comment = excluded.comment""",
            (d, report_type, comment),
        )
        await db.commit()


async def get_report_comment(report_date: date, report_type: str) -> str | None:
    """Получить комментарий к отчёту за дату (daily или evening)."""
    d = report_date.strftime("%Y-%m-%d")
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute(
            "SELECT comment FROM report_comments WHERE report_date = ? AND report_type = ?",
            (d, report_type),
        )
        row = await cursor.fetchone()
        return row[0] if row and row[0] else None


async def clear_plans_and_summaries() -> None:
    """Очистить все планы и данные сводок (отчёты, корректировки, флаги вечерней отправки)."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("DELETE FROM plans")
        await db.execute("DELETE FROM reports")
        await db.execute("DELETE FROM daily_corrections")
        await db.execute("DELETE FROM evening_summary_sent")
        await db.execute("DELETE FROM report_comments")
        await db.commit()


async def add_report(
    direction: str,
    amount: float,
    comment: str,
    user_id: int = None,
    username: str = None,
) -> int:
    """Добавить отчёт о продажах. Дата и время — по Москве, чтобы сводка за день совпадала с отображением."""
    now = datetime.now(MOSCOW)
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute(
            """
            INSERT INTO reports (direction, amount, comment, report_date, report_time, user_id, username)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                direction,
                amount,
                comment or "",
                now.strftime("%Y-%m-%d"),
                now.strftime("%H:%M"),
                user_id,
                username,
            ),
        )
        await db.commit()
        return cursor.lastrowid


async def get_daily_summary(report_date: date = None) -> list[dict]:
    """Получить сводку по направлениям за день (по умолчанию — сегодня по Москве)."""
    if report_date is None:
        report_date = today_moscow()
    date_str = report_date.strftime("%Y-%m-%d")

    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT direction, SUM(amount) as total, COUNT(*) as count
            FROM reports
            WHERE report_date = ?
            GROUP BY direction
            ORDER BY total DESC
            """,
            (date_str,),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def get_reports_by_time(report_date: date, report_time: str) -> list[dict]:
    """Получить отчёты за конкретный слот времени (фокусный отчёт)."""
    date_str = report_date.strftime("%Y-%m-%d")
    # Определяем временной диапазон (например, 14:00 = отчёты с 12:00 до 14:00)
    hour = int(report_time.split(":")[0])
    start_hour = max(0, hour - 2)
    time_start = f"{start_hour:02d}:00"
    time_end = report_time

    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT direction, amount, comment, report_time, username
            FROM reports
            WHERE report_date = ? AND report_time >= ? AND report_time <= ?
            ORDER BY report_time
            """,
            (date_str, time_start, time_end),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def get_total_for_period(report_date: date, report_time: str = None) -> float:
    """Общая сумма за период (день или до указанного времени)."""
    date_str = report_date.strftime("%Y-%m-%d")
    async with aiosqlite.connect(DATABASE_PATH) as db:
        if report_time:
            hour = int(report_time.split(":")[0])
            start_hour = max(0, hour - 2)
            time_start = f"{start_hour:02d}:00"
            cursor = await db.execute(
                """
                SELECT COALESCE(SUM(amount), 0) FROM reports
                WHERE report_date = ? AND report_time >= ? AND report_time <= ?
                """,
                (date_str, time_start, report_time),
            )
        else:
            cursor = await db.execute(
                "SELECT COALESCE(SUM(amount), 0) FROM reports WHERE report_date = ?",
                (date_str,),
            )
        row = await cursor.fetchone()
        return row[0] or 0.0


async def upsert_month_plan(
    direction: str,
    year: int,
    month: int,
    plan_amount: float,
) -> None:
    """Создать или обновить месячный план по направлению."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            """
            INSERT INTO plans (direction, year, month, plan)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(direction, year, month)
            DO UPDATE SET plan = excluded.plan,
                          updated_at = CURRENT_TIMESTAMP
            """,
            (direction, year, month, plan_amount),
        )
        await db.commit()


async def get_month_plans(report_date: date | None = None) -> dict:
    """Получить планы по всем направлениям на месяц (дата по умолчанию — сегодня по Москве)."""
    if report_date is None:
        report_date = today_moscow()
    year = report_date.year
    month = report_date.month

    async with aiosqlite.connect(DATABASE_PATH) as db:
        async with db.execute(
            """
            SELECT direction, plan
            FROM plans
            WHERE year = ? AND month = ?
            """,
            (year, month),
        ) as cursor:
            rows = await cursor.fetchall()
            result = {}
            for row in rows:
                direction = row[0]
                if isinstance(direction, str):
                    direction = direction.strip()
                plan_val = row[1]
                if plan_val is None:
                    plan_val = 0.0
                else:
                    plan_val = float(plan_val)
                result[direction] = plan_val
            return result


async def get_month_summary(report_date: date | None = None) -> list[dict]:
    """Сводка по направлениям за месяц (факт). По умолчанию — текущий месяц по Москве."""
    if report_date is None:
        report_date = today_moscow()
    ym = report_date.strftime("%Y-%m")

    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """
            SELECT direction, SUM(amount) AS total, COUNT(*) AS count
            FROM reports
            WHERE substr(report_date, 1, 7) = ?
            GROUP BY direction
            ORDER BY total DESC
            """,
            (ym,),
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def set_evening_notification(chat_id: int, enabled: bool) -> None:
    """Включить/выключить вечерний отчёт для чата."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            """
            INSERT INTO evening_notifications (chat_id, enabled)
            VALUES (?, ?)
            ON CONFLICT(chat_id)
            DO UPDATE SET enabled = excluded.enabled,
                          added_at = CURRENT_TIMESTAMP
            """,
            (chat_id, 1 if enabled else 0),
        )
        await db.commit()


async def set_evening_summary_sent(report_date: date) -> None:
    """Отметить, что вечерняя сводка за дату отправлена в чат."""
    date_str = report_date.strftime("%Y-%m-%d")
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            "INSERT OR REPLACE INTO evening_summary_sent (report_date) VALUES (?)",
            (date_str,),
        )
        await db.commit()


async def was_evening_summary_sent(report_date: date) -> bool:
    """Проверить, отправлялась ли вечерняя сводка за дату."""
    date_str = report_date.strftime("%Y-%m-%d")
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute(
            "SELECT 1 FROM evening_summary_sent WHERE report_date = ?",
            (date_str,),
        )
        return (await cursor.fetchone()) is not None


async def count_evening_reports_today(report_date: date) -> int:
    """Число записей «Вечерний отчёт» за дату (0 = не начинали, 1+ = начали заполнять)."""
    date_str = report_date.strftime("%Y-%m-%d")
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute(
            "SELECT COUNT(*) FROM reports WHERE report_date = ? AND comment = ?",
            (date_str, "Вечерний отчёт"),
        )
        return (await cursor.fetchone())[0]


async def get_evening_chats() -> list[int]:
    """Получить список чатов с включённым вечерним отчётом."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        async with db.execute(
            "SELECT chat_id FROM evening_notifications WHERE enabled = 1"
        ) as cursor:
            rows = await cursor.fetchall()
            return [row[0] for row in rows]


async def set_daily_correction(report_date: date, direction: str, value: float) -> None:
    """Сохранить корректировку по направлению на дату (итог за день)."""
    date_str = report_date.strftime("%Y-%m-%d")
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            """
            INSERT INTO daily_corrections (report_date, direction, value)
            VALUES (?, ?, ?)
            ON CONFLICT(report_date, direction)
            DO UPDATE SET value = excluded.value, updated_at = CURRENT_TIMESTAMP
            """,
            (date_str, direction, value),
        )
        await db.commit()


async def get_daily_corrections(report_date: date) -> dict[str, float]:
    """Получить корректировки за день: {направление: значение}."""
    date_str = report_date.strftime("%Y-%m-%d")
    async with aiosqlite.connect(DATABASE_PATH) as db:
        async with db.execute(
            "SELECT direction, value FROM daily_corrections WHERE report_date = ?",
            (date_str,),
        ) as cursor:
            rows = await cursor.fetchall()
            return {row[0]: row[1] for row in rows}


async def add_pending_request(user_id: int, username: str = None, first_name: str = None) -> None:
    """Добавить или обновить заявку на доступ."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            """
            INSERT INTO pending_access_requests (user_id, username, first_name, requested_at)
            VALUES (?, ?, ?, CURRENT_TIMESTAMP)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                first_name = excluded.first_name,
                requested_at = CURRENT_TIMESTAMP
            """,
            (user_id, username or "", first_name or ""),
        )
        await db.commit()


async def get_pending_requests() -> list[dict]:
    """Список заявок на доступ."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT user_id, username, first_name, requested_at FROM pending_access_requests ORDER BY requested_at"
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


async def remove_pending_request(user_id: int) -> None:
    """Удалить заявку (после подтверждения или отклонения)."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute("DELETE FROM pending_access_requests WHERE user_id = ?", (user_id,))
        await db.commit()


async def add_allowed_user(user_id: int, username: str = None):
    """Добавить разрешённого пользователя."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        await db.execute(
            """
            INSERT OR REPLACE INTO allowed_users (user_id, username, added_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            """,
            (user_id, username),
        )
        await db.commit()


async def get_plans_export(year: int, month: int) -> list[dict]:
    """Планы на месяц для экспорта (direction, year, month, plan)."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT direction, year, month, plan FROM plans WHERE year = ? AND month = ? ORDER BY direction",
            (year, month),
        ) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def get_reports_export(year: int, month: int) -> list[dict]:
    """Отчёты за месяц для экспорта (report_date, direction, amount, comment, report_time)."""
    ym = f"{year}-{month:02d}"
    async with aiosqlite.connect(DATABASE_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            """SELECT report_date, direction, amount, comment, report_time 
               FROM reports WHERE substr(report_date, 1, 7) = ? ORDER BY report_date, report_time""",
            (ym,),
        ) as cur:
            rows = await cur.fetchall()
            return [dict(r) for r in rows]


async def is_allowed_user(user_id: int) -> bool:
    """Проверить, разрешён ли пользователь (в списке подтверждённых). Если список пуст — только РТТ входят через user_has_access."""
    async with aiosqlite.connect(DATABASE_PATH) as db:
        cursor = await db.execute(
            "SELECT 1 FROM allowed_users WHERE user_id = ?",
            (user_id,),
        )
        return (await cursor.fetchone()) is not None
