"""
Telegram-бот для ежедневной отчётности по продажам.
Поддерживает несколько направлений и фокусные отчёты несколько раз в день.
"""
import calendar
import csv
import io
import math
import logging
from datetime import date, datetime, time as dt_time
from zoneinfo import ZoneInfo
from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InputFile,
)
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from config import (
    BOT_TOKEN,
    REPORT_CHAT_IDS,
    RTT_USER_IDS,
    SALES_DIRECTIONS,
    REPORT_SCHEDULE,
    DIRECTION_UNITS,
    DIRECTION_BLOCKS,
    DIRECTION_SHORT,
    today_moscow,
)
from database import (
    init_db,
    add_report,
    add_allowed_user,
    get_daily_summary,
    get_reports_by_time,
    is_allowed_user,
    get_month_summary,
    get_month_plans,
    upsert_month_plan,
    set_evening_notification,
    get_evening_chats,
    get_daily_corrections,
    set_daily_correction,
    set_evening_summary_sent,
    was_evening_summary_sent,
    count_evening_reports_today,
    add_pending_request,
    get_pending_requests,
    remove_pending_request,
    get_plans_export,
    get_reports_export,
    set_report_comment,
    get_report_comment,
)

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# Состояния для FSM (ввод суммы и комментария)
USER_STATE = {}


def _is_piece_direction(direction: str) -> bool:
    """Направление учитывается в штуках, а не в деньгах."""
    return DIRECTION_UNITS.get(direction) == "шт"


def _format_amount(direction: str, amount: float) -> str:
    """Форматирование значения с учётом единиц измерения. Без десятых — при дробной части округление в большую сторону."""
    if math.isfinite(amount) and amount != int(amount):
        amount = math.ceil(amount)
    else:
        amount = round(amount)
    n = int(amount)
    unit = DIRECTION_UNITS.get(direction, "₽")
    if unit == "шт":
        return f"{n:,} {unit}"
    return f"{n:,} ₽"


def get_keyboard_directions():
    """Клавиатура выбора направления."""
    buttons = [
        [InlineKeyboardButton(d, callback_data=f"dir_{d}")] for d in SALES_DIRECTIONS
    ]
    return InlineKeyboardMarkup(buttons)


def get_evening_skip_cancel_keyboard() -> InlineKeyboardMarkup:
    """Кнопки Skip и Отмена под сообщением вечернего отчёта."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Skip", callback_data="ev_skip"),
            InlineKeyboardButton("Отмена", callback_data="ev_cancel"),
        ],
    ])


def get_daily_skip_cancel_keyboard() -> InlineKeyboardMarkup:
    """Кнопки Skip и Отмена под сообщением дневного отчёта."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Skip", callback_data="daily_skip"),
            InlineKeyboardButton("Отмена", callback_data="daily_cancel"),
        ],
    ])


def get_summary_correct_keyboard() -> InlineKeyboardMarkup:
    """Кнопка «Корректировать» под сводкой."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Корректировать", callback_data="summary_corr")],
    ])


def get_correction_directions_keyboard() -> InlineKeyboardMarkup:
    """Кнопки выбора направления для корректировки (по блокам)."""
    buttons = []
    for block in DIRECTION_BLOCKS:
        for direction in block:
            buttons.append([InlineKeyboardButton(direction, callback_data=f"corr_{direction}")])
    return InlineKeyboardMarkup(buttons)


def get_plans_actions_keyboard() -> InlineKeyboardMarkup:
    """Кнопки для РТТ: корректировать одно направление или внести планы на месяц."""
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✏️ Корректировать направление", callback_data="plan_edit")],
        [InlineKeyboardButton("➕ Внести планы на месяц", callback_data="plan_new_month")],
    ])


def get_plan_edit_directions_keyboard() -> InlineKeyboardMarkup:
    """Выбор направления для редактирования плана (по блокам)."""
    buttons = []
    for block in DIRECTION_BLOCKS:
        for direction in block:
            buttons.append([InlineKeyboardButton(direction, callback_data=f"plan_edit_{direction}")])
    return InlineKeyboardMarkup(buttons)


def get_plan_skip_cancel_keyboard() -> InlineKeyboardMarkup:
    """Skip и Отмена при пошаговом вводе планов."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("Skip (0)", callback_data="plan_skip"),
            InlineKeyboardButton("Отмена", callback_data="plan_cancel"),
        ],
    ])


def get_confirm_send_keyboard(report_type: str) -> InlineKeyboardMarkup:
    """Кнопки подтверждения отправки сводки в чат: Да / Не отправлять."""
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Отправить в чат", callback_data=f"send_confirm_{report_type}_yes"),
            InlineKeyboardButton("❌ Не отправлять", callback_data=f"send_confirm_{report_type}_no"),
        ],
    ])


def get_main_menu_keyboard(user_id: int | None = None) -> ReplyKeyboardMarkup:
    """Главное меню с кнопками. У РТТ — «Уведомления» и «Заявки»; у ПК их нет."""
    rows = [
        [KeyboardButton("➕ Отчёт"), KeyboardButton("📊 Сводка")],
        [KeyboardButton("📋 Дневной отчёт"), KeyboardButton("🌙 Вечерний отчёт")],
        [KeyboardButton("📈 Планы"), KeyboardButton("📌 Как дела?")],
    ]
    if user_id is not None and is_rtt(user_id):
        rows[-1].append(KeyboardButton("🔔 Уведомления"))
        rows.append([KeyboardButton("📋 Заявки на доступ")])
    return ReplyKeyboardMarkup(rows, resize_keyboard=True)


def is_rtt(user_id: int) -> bool:
    """Пользователь имеет роль РТТ (руководитель торговой точки): может корректировать планы и сводку."""
    return user_id in RTT_USER_IDS


async def user_has_access(user_id: int) -> bool:
    """Есть ли у пользователя доступ: РТТ по умолчанию или подтверждённый пользователь (ПК)."""
    if user_id in RTT_USER_IDS:
        return True
    return await is_allowed_user(user_id)


async def check_access(update: Update, context: ContextTypes.DEFAULT_TYPE) -> bool:
    """Проверка доступа пользователя."""
    user_id = update.effective_user.id if update.effective_user else 0
    if not await user_has_access(user_id):
        msg = (
            "⛔ У вас нет доступа к боту.\n\n"
            "Отправьте /start — заявка уйдёт РТТ. После подтверждения доступ откроется."
        )
        if update.message:
            await update.message.reply_text(msg)
        return False
    return True


async def cmd_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /start. Если нет доступа — подача заявки РТТ на подтверждение."""
    user_id = update.effective_user.id if update.effective_user else 0
    if not await user_has_access(user_id):
        user = update.effective_user
        await add_pending_request(
            user_id,
            username=user.username if user else None,
            first_name=user.first_name if user else None,
        )
        await update.message.reply_text(
            "📋 *Заявка на доступ отправлена.*\n\n"
            "Ожидайте подтверждения от РТТ. "
            "После одобрения вам придёт уведомление — нажмите /start.",
            parse_mode="Markdown",
        )
        return
    text = """
📊 *Бот отчётности по продажам*

Доступные команды:
/report — Добавить отчёт о продажах
/summary [DD.MM] — Сводка за день (без даты — сегодня; в боте полная, в чат краткая)
/daily — Дневной отчёт (после заполнения — подтверждение отправки в чат)
/plans — Показать планы на месяц
/setplan — Задать/изменить план (только РТТ)
/export plans [YYYY-MM] или reports [YYYY-MM] — выгрузка в CSV (только РТТ)
/pending — Заявки на доступ (только РТТ)
/help — Справка

Кнопка «Как дела?» — краткая строка: % плана и направления в норме.

*Роли:* РТТ — корректировка планов и сводки; ПК — отчёты и просмотр.

*Дневной отчёт:* за 10 мин (11:50, 15:50, 18:50) — уведомление в чат; в 12:00, 16:00 и 19:00 — напоминание заполнить. Заполняете в боте (ЛС) → сводка уходит в чат.
*Вечерний:* в 20:50 и 21:00 — напоминания (если отчёт не заполнен); сводка в чат — только после заполнения «Вечерний отчёт» в боте.
    """
    await update.message.reply_text(
        text,
        parse_mode="Markdown",
        reply_markup=get_main_menu_keyboard(user_id),
    )


async def cmd_help(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Команда /help."""
    if update.message and not await check_access(update, context):
        return
    text = """
📖 *Справка*

*Добавить отчёт:*
/report — выберите направление, введите сумму (и опционально комментарий)

*Направления (планы):*
- Sim (в шт)
- Золото/абонемент (в шт)
- Мнп (в шт)
- Combo (в шт)
- Страховки (в сумме)
- Аксессуары (в сумме)
- Смартфоны (в сумме)
- Услуги (в сумме)
- Кредиты (в сумме)
- Заявка на кредит (в шт)
- Wink подписка (в сумме)
- Фокусное оборудование (в сумме)
- Домашний интернет (в шт)

*Сводка:* /summary — полная сводка в боте (все показатели); в чат отправляется краткая (план/день и выполнено за день). РТТ может корректировать.

*Роли:* РТТ — планы и корректировка сводки; ПК — только заполнять отчёты и смотреть.

*Дневной отчёт:* за 10 мин до отчёта (11:50, 15:50, 18:50) — уведомление в чат; в 12:00, 16:00 и 19:00 — напоминание заполнить в боте (ЛС), сводка в чат после заполнения. *Вечерний:* в 20:50 и 21:00 — напоминания (если не заполнен или не завершён); сводка в чат — только после заполнения «Вечерний отчёт» в боте.

*Вечерний отчёт сотрудника:*
/evening — по очереди задаёт вопросы по всем показателям, вы отвечаете цифрами

*Планы:*
/plans — показать планы на месяц
/setplan YYYY-MM <направление> <сумма> — задать/изменить план
    """
    await update.message.reply_text(
        text,
        parse_mode="Markdown",
        reply_markup=get_main_menu_keyboard(update.effective_user.id if update.effective_user else None),
    )


async def cmd_pending(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Заявки на доступ (только РТТ): список и кнопки Подтвердить/Отклонить."""
    if not await check_access(update, context):
        return
    user_id = update.effective_user.id if update.effective_user else 0
    if not is_rtt(user_id):
        await update.message.reply_text("⛔ Только РТТ может просматривать и подтверждать заявки.")
        return

    pending = await get_pending_requests()
    if not pending:
        await update.message.reply_text(
            "Заявок на доступ нет.",
            reply_markup=get_main_menu_keyboard(user_id),
        )
        return

    lines = ["📋 *Заявки на доступ к боту:*\n"]
    buttons = []
    for r in pending:
        uid = r["user_id"]
        name = (r.get("first_name") or "").strip() or "—"
        uname = (r.get("username") or "").strip()
        uname = f" (@{uname})" if uname else ""
        lines.append(f"• {name}{uname} (ID: {uid})")
        buttons.append([
            InlineKeyboardButton(f"✓ Подтвердить {uid}", callback_data=f"access_confirm_{uid}"),
            InlineKeyboardButton(f"✗ Отклонить", callback_data=f"access_reject_{uid}"),
        ])

    await update.message.reply_text(
        "\n".join(lines),
        parse_mode="Markdown",
        reply_markup=InlineKeyboardMarkup(buttons),
    )


async def callback_access_confirm_reject(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка Подтвердить/Отклонить заявки на доступ."""
    query = update.callback_query
    who_id = update.effective_user.id if update.effective_user else 0
    if not is_rtt(who_id):
        await query.answer("Только РТТ может подтверждать заявки.", show_alert=True)
        return

    data = query.data or ""
    if data.startswith("access_confirm_"):
        try:
            target_uid = int(data.replace("access_confirm_", ""))
        except ValueError:
            await query.answer()
            return
        pending = await get_pending_requests()
        if not any(r["user_id"] == target_uid for r in pending):
            await query.answer("Заявка уже обработана.", show_alert=True)
            return
        await query.answer()
        # Найти username для добавления
        req = next((r for r in pending if r["user_id"] == target_uid), None)
        if not req:
            return
        await add_allowed_user(target_uid, req.get("username"))
        await remove_pending_request(target_uid)
        try:
            await context.bot.send_message(
                target_uid,
                "✅ Вам предоставлен доступ к боту. Нажмите /start для начала работы.",
            )
        except Exception as e:
            logger.warning("Could not notify user %s: %s", target_uid, e)
        name = (req.get("first_name") or "").strip() or str(target_uid)
        await query.edit_message_text(f"✅ Пользователь {name} (ID: {target_uid}) подтверждён.")
    elif data.startswith("access_reject_"):
        try:
            target_uid = int(data.replace("access_reject_", ""))
        except ValueError:
            await query.answer()
            return
        await query.answer()
        await remove_pending_request(target_uid)
        await query.edit_message_text(f"Заявка пользователя ID {target_uid} отклонена.")


async def cmd_report(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Начать добавление отчёта."""
    if not await check_access(update, context):
        return
    await update.message.reply_text(
        "Выберите направление продаж:",
        reply_markup=get_keyboard_directions(),
    )


async def callback_direction(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка выбора направления."""
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id if update.effective_user else 0
    if not await user_has_access(user_id):
        await query.edit_message_text("⛔ Нет доступа.")
        return

    if query.data.startswith("dir_"):
        direction = query.data[4:]
        USER_STATE[user_id] = {"mode": "single", "direction": direction, "step": "amount"}
        await query.edit_message_text(
            f"📌 Направление: *{direction}*\n\nВведите сумму продаж (число):",
            parse_mode="Markdown",
        )


async def cmd_notify(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Меню настроек уведомлений для текущего чата. Только РТТ."""
    if not await check_access(update, context):
        return
    user_id = update.effective_user.id if update.effective_user else 0
    if not is_rtt(user_id):
        await update.message.reply_text("⛔ Управление уведомлениями доступно только РТТ.")
        return

    keyboard = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("✅ Включить вечерний отчёт", callback_data="notif_on")],
            [InlineKeyboardButton("🚫 Выключить вечерний отчёт", callback_data="notif_off")],
        ]
    )
    await update.message.reply_text(
        "Настройка уведомлений для этого чата:\n"
        "— вечерний финальный отчёт за день с остатком до плана.",
        reply_markup=keyboard,
    )


async def callback_summary_correct(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Корректировка сводки: выбор направления и ввод нового значения. Только РТТ."""
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id if update.effective_user else 0
    chat_id = query.message.chat_id

    if not await user_has_access(user_id):
        return
    if not is_rtt(user_id):
        await query.answer("⛔ Только РТТ может корректировать сводку.", show_alert=True)
        return

    data = query.data or ""

    if data == "summary_corr":
        await query.edit_message_reply_markup(reply_markup=None)
        await context.bot.send_message(
            chat_id,
            "Выберите направление для корректировки (итог за сегодня):",
            reply_markup=get_correction_directions_keyboard(),
        )
        return

    if data.startswith("corr_"):
        direction = data[5:]
        if direction not in SALES_DIRECTIONS:
            await query.answer("Неизвестное направление.", show_alert=True)
            return
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        USER_STATE[user_id] = {
            "mode": "correction",
            "direction": direction,
        }
        unit = DIRECTION_UNITS.get(direction, "₽")
        await context.bot.send_message(
            chat_id,
            f"✏️ Корректировка *{direction}* ({unit}).\nВведите новое значение (итог за сегодня):",
            parse_mode="Markdown",
        )


async def callback_plan_actions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Планы: корректировать одно направление или пошаговый ввод планов на месяц. Только РТТ."""
    query = update.callback_query
    user_id = update.effective_user.id if update.effective_user else 0
    chat_id = query.message.chat_id

    if not is_rtt(user_id):
        await query.answer("Только РТТ может редактировать планы.", show_alert=True)
        return

    data = query.data or ""

    if data == "plan_edit":
        await query.answer()
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await context.bot.send_message(
            chat_id,
            "Выберите направление для корректировки плана (текущий месяц):",
            reply_markup=get_plan_edit_directions_keyboard(),
        )
        return

    if data.startswith("plan_edit_"):
        direction = data.replace("plan_edit_", "", 1)
        if direction not in SALES_DIRECTIONS:
            await query.answer("Неизвестное направление.", show_alert=True)
            return
        await query.answer()
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        today = today_moscow()
        USER_STATE[user_id] = {
            "mode": "plan_edit",
            "direction": direction,
            "year": today.year,
            "month": today.month,
        }
        unit = DIRECTION_UNITS.get(direction, "₽")
        await context.bot.send_message(
            chat_id,
            f"✏️ План для *{direction}* ({unit}) на {today.strftime('%Y-%m')}. Введите значение:",
            parse_mode="Markdown",
        )
        return

    if data == "plan_new_month":
        await query.answer()
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        USER_STATE[user_id] = {"mode": "plan_new_month", "step": "month"}
        await context.bot.send_message(
            chat_id,
            "📅 Введите месяц в формате *YYYY-MM* (например 2025-03):",
            parse_mode="Markdown",
        )
        return

    if data == "plan_skip":
        await query.answer()
        state = USER_STATE.get(user_id)
        if not state or state.get("mode") != "plan_new_month" or state.get("step") != "values":
            return
        directions = SALES_DIRECTIONS
        index = state["index"]
        current_direction = directions[index]
        state["values"][current_direction] = 0
        index += 1
        state["index"] = index
        if index >= len(directions):
            for d, val in state["values"].items():
                await upsert_month_plan(d, state["year"], state["month"], val)
            del USER_STATE[user_id]
            ym = f"{state['year']}-{state['month']:02d}"
            await context.bot.send_message(
                chat_id,
                f"✅ Планы на {ym} сохранены.",
                reply_markup=get_main_menu_keyboard(user_id),
            )
            return
        next_direction = directions[index]
        unit = DIRECTION_UNITS.get(next_direction, "₽")
        await context.bot.send_message(
            chat_id,
            f"→ {next_direction} ({unit}):",
            reply_markup=get_plan_skip_cancel_keyboard(),
        )
        return

    if data == "plan_cancel":
        await query.answer()
        if user_id in USER_STATE and USER_STATE[user_id].get("mode") in ("plan_edit", "plan_new_month"):
            del USER_STATE[user_id]
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        await context.bot.send_message(
            chat_id,
            "Редактирование планов отменено.",
            reply_markup=get_main_menu_keyboard(user_id),
        )
        return


async def callback_evening_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка кнопок Skip и Отмена в вечернем отчёте."""
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id if update.effective_user else 0
    chat_id = query.message.chat_id

    if not await user_has_access(user_id):
        return

    state = USER_STATE.get(user_id)
    if not state or state.get("mode") != "evening":
        await query.edit_message_text("Сессия завершена. Начните вечерний отчёт заново.")
        return

    directions = state["directions"]
    index = state["index"]

    if query.data == "ev_cancel":
        del USER_STATE[user_id]
        await context.bot.send_message(
            chat_id,
            "Вечерний отчёт отменён.",
            reply_markup=get_main_menu_keyboard(user_id),
        )
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        return

    if query.data == "ev_skip":
        index += 1
        state["index"] = index

        if index >= len(directions):
            username = update.effective_user.username if update.effective_user else None
            total_reports = 0
            for direction, amount in state["values"].items():
                await add_report(
                    direction=direction,
                    amount=amount,
                    comment="Вечерний отчёт",
                    user_id=user_id,
                    username=username,
                )
                total_reports += 1
            today = today_moscow()
            USER_STATE[user_id] = {"mode": "report_comment", "type": "evening", "date": today}
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass
            await context.bot.send_message(
                chat_id,
                f"✅ Вечерний отчёт заполнен. Записано показателей: {total_reports}.\n\nДобавить комментарий к отчёту? (или /skip)",
            )
            return

        next_direction = directions[index]
        unit = DIRECTION_UNITS.get(next_direction, "₽")
        await context.bot.send_message(
            chat_id,
            f"→ {next_direction} ({unit}):",
            reply_markup=get_evening_skip_cancel_keyboard(),
        )
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass


async def callback_daily_buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка кнопок Skip и Отмена в дневном отчёте. После завершения — отправка сводки в чат."""
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id if update.effective_user else 0
    chat_id = query.message.chat_id

    if not await user_has_access(user_id):
        return

    state = USER_STATE.get(user_id)
    if not state or state.get("mode") != "daily":
        await query.edit_message_text("Сессия завершена. Начните дневной отчёт заново.")
        return

    directions = state["directions"]
    index = state["index"]

    if query.data == "daily_cancel":
        del USER_STATE[user_id]
        await context.bot.send_message(
            chat_id,
            "Дневной отчёт отменён.",
            reply_markup=get_main_menu_keyboard(user_id),
        )
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass
        return

    if query.data == "daily_skip":
        index += 1
        state["index"] = index

        if index >= len(directions):
            username = update.effective_user.username if update.effective_user else None
            for direction, amount in state["values"].items():
                await add_report(
                    direction=direction,
                    amount=amount,
                    comment="Дневной отчёт",
                    user_id=user_id,
                    username=username,
                )
            today = today_moscow()
            USER_STATE[user_id] = {"mode": "report_comment", "type": "daily", "date": today}
            try:
                await query.edit_message_reply_markup(reply_markup=None)
            except Exception:
                pass
            await context.bot.send_message(
                chat_id,
                "✅ Дневной отчёт заполнен.\n\nДобавить комментарий к отчёту? (или /skip)",
            )
            return

        next_direction = directions[index]
        unit = DIRECTION_UNITS.get(next_direction, "₽")
        await context.bot.send_message(
            chat_id,
            f"→ {next_direction} ({unit}):",
            reply_markup=get_daily_skip_cancel_keyboard(),
        )
        try:
            await query.edit_message_reply_markup(reply_markup=None)
        except Exception:
            pass


async def callback_confirm_send(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Подтверждение отправки сводки в чат после заполнения дневного/вечернего отчёта."""
    query = update.callback_query
    await query.answer()
    user_id = update.effective_user.id if update.effective_user else 0
    chat_id = query.message.chat_id
    data = query.data or ""
    if not data.startswith("send_confirm_") or ("_yes" not in data and "_no" not in data):
        return
    state = USER_STATE.get(user_id)
    if not state or state.get("mode") != "confirm_send":
        await query.edit_message_text("Сессия истекла.")
        return
    report_type = state.get("type") or data.replace("send_confirm_", "").replace("_yes", "").replace("_no", "")
    send_it = "_yes" in data
    del USER_STATE[user_id]
    if send_it:
        if report_type == "daily":
            target_date = state.get("date") or today_moscow()
            sent = await _send_daily_summary_to_chats(context, target_date)
            msg = "Сводка отправлена в чат." if sent else "Не удалось отправить (проверьте REPORT_CHAT_IDS)."
        else:
            sent = await _send_evening_summary_to_chats(context)
            if sent:
                await set_evening_summary_sent(state.get("date") or today_moscow())
            msg = "Сводка отправлена в чат." if sent else "Не удалось отправить."
    else:
        msg = "Сводка не отправлена."
    await query.edit_message_text(f"✅ {msg}")
    await context.bot.send_message(chat_id, "Готово.", reply_markup=get_main_menu_keyboard(user_id))


async def callback_notifications(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка нажатий на кнопки уведомлений. Только РТТ."""
    query = update.callback_query
    user_id = update.effective_user.id if update.effective_user else 0
    if not is_rtt(user_id):
        await query.answer("Только РТТ может управлять уведомлениями.", show_alert=True)
        return
    await query.answer()
    chat = query.message.chat
    chat_id = chat.id

    if query.data == "notif_on":
        await set_evening_notification(chat_id, True)
        await query.edit_message_text(
            "✅ Вечерний финальный отчёт для этого чата *включён*.\n"
            "Бот будет присылать итог дня и остаток до выполнения плана.",
            parse_mode="Markdown",
        )
    elif query.data == "notif_off":
        await set_evening_notification(chat_id, False)
        await query.edit_message_text(
            "🚫 Вечерний финальный отчёт для этого чата *выключен*.",
            parse_mode="Markdown",
        )


async def cmd_evening(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Запуск вечернего отчёта — поочерёдный опрос по всем показателям."""
    if not await check_access(update, context):
        return

    # Вечерний отчёт заполняется только в личных сообщениях с ботом
    chat = update.effective_chat
    if chat and chat.type != "private":
        await update.message.reply_text(
            "Для заполнения вечернего отчёта напишите, пожалуйста, боту в личные сообщения.",
        )
        return

    user_id = update.effective_user.id if update.effective_user else 0

    # Строим список направлений в том же порядке блоков
    directions: list[str] = []
    for block in DIRECTION_BLOCKS:
        for direction in block:
            if direction in SALES_DIRECTIONS:
                directions.append(direction)

    if not directions:
        await update.message.reply_text("Нет настроенных направлений для вечернего отчёта.")
        return

    USER_STATE[user_id] = {
        "mode": "evening",
        "directions": directions,
        "index": 0,
        "values": {},
    }

    first_direction = directions[0]
    unit = DIRECTION_UNITS.get(first_direction, "₽")
    await update.message.reply_text(
        "🌙 Вечерний отчёт. Число или кнопки ниже.\n"
        f"→ {first_direction} ({unit}):",
        reply_markup=get_evening_skip_cancel_keyboard(),
    )


async def cmd_daily(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Дневной отчёт: заполняется сотрудником в ЛС; после заполнения сводка уходит в чат."""
    if not await check_access(update, context):
        return

    chat = update.effective_chat
    if chat and chat.type != "private":
        await update.message.reply_text(
            "Дневной отчёт заполняется в личных сообщениях с ботом. Напишите боту в ЛС.",
        )
        return

    user_id = update.effective_user.id if update.effective_user else 0
    directions: list[str] = []
    for block in DIRECTION_BLOCKS:
        for direction in block:
            if direction in SALES_DIRECTIONS:
                directions.append(direction)

    if not directions:
        await update.message.reply_text("Нет настроенных направлений.")
        return

    USER_STATE[user_id] = {
        "mode": "daily",
        "directions": directions,
        "index": 0,
        "values": {},
    }

    first_direction = directions[0]
    unit = DIRECTION_UNITS.get(first_direction, "₽")
    await update.message.reply_text(
        "📋 Дневной отчёт. Число или кнопки ниже.\n"
        f"→ {first_direction} ({unit}):",
        reply_markup=get_daily_skip_cancel_keyboard(),
    )


async def _send_daily_summary_to_chats(context, target_date: date, time_slot: str = "") -> bool:
    """Отправить дневную сводку в рабочие чаты (REPORT_CHAT_IDS). Краткий формат: План/день | Выполнено за день."""
    title = f"📊 *Дневной отчёт*{f' на {time_slot}' if time_slot else ''}"
    text = await build_summary_text(target_date, title=title, variant="day_chat")
    sent = False
    for cid in REPORT_CHAT_IDS:
        try:
            await context.bot.send_message(cid, text, parse_mode="Markdown")
            sent = True
        except Exception as e:
            logger.warning("Не удалось отправить дневной отчёт в чат %s: %s", cid, e)
    return sent


async def _send_evening_summary_to_chats(context) -> bool:
    """Отправить вечерний финальный отчёт в подписанные чаты (get_evening_chats). Возвращает True если хотя бы в один чат отправлено."""
    today = today_moscow()
    text = await build_summary_text(
        today,
        title="🌙 *Вечерний финальный отчёт*",
        variant="evening",
    )
    chat_ids = await get_evening_chats()
    sent = False
    for chat_id in chat_ids:
        try:
            await context.bot.send_message(chat_id, text, parse_mode="Markdown")
            sent = True
            logger.info(f"Sent evening summary to chat {chat_id}")
        except Exception as e:
            logger.error(f"Failed to send evening summary to {chat_id}: {e}")
    if sent:
        await set_evening_summary_sent(today)
    return sent


async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обработка ввода суммы и комментария."""
    user_id = update.effective_user.id if update.effective_user else 0
    if not await user_has_access(user_id):
        return

    text = update.message.text.strip()

    # Обработка нажатий на кнопки главного меню — всегда, чтобы кнопки работали в любом режиме
    if text == "➕ Отчёт":
        if user_id in USER_STATE:
            del USER_STATE[user_id]
        await cmd_report(update, context)
        return
    if text == "📊 Сводка":
        if user_id in USER_STATE:
            del USER_STATE[user_id]
        await cmd_summary(update, context)
        return
    if text == "📋 Дневной отчёт":
        if user_id in USER_STATE:
            del USER_STATE[user_id]
        await cmd_daily(update, context)
        return
    if text == "🌙 Вечерний отчёт":
        if user_id in USER_STATE:
            del USER_STATE[user_id]
        await cmd_evening(update, context)
        return
    if text == "📈 Планы":
        if user_id in USER_STATE:
            del USER_STATE[user_id]
        await cmd_plans(update, context)
        return
    if text == "🔔 Уведомления":
        if user_id in USER_STATE:
            del USER_STATE[user_id]
        await cmd_notify(update, context)
        return
    if text == "📋 Заявки на доступ":
        if user_id in USER_STATE:
            del USER_STATE[user_id]
        await cmd_pending(update, context)
        return
    if text == "📌 Как дела?":
        if user_id in USER_STATE:
            del USER_STATE[user_id]
        await cmd_today_quick(update, context)
        return

    state = USER_STATE.get(user_id)
    if not state:
        return

    if text.strip().lower() == "/cancel" and state.get("mode") in ("plan_edit", "plan_new_month"):
        del USER_STATE[user_id]
        await update.message.reply_text("Редактирование планов отменено.", reply_markup=get_main_menu_keyboard(user_id))
        return

    # Режим корректировки плана (одно направление, только РТТ)
    if state.get("mode") == "plan_edit":
        if not is_rtt(user_id):
            del USER_STATE[user_id]
            return
        direction = state.get("direction", "")
        try:
            value = float(text.replace(",", "."))
            if value < 0:
                await update.message.reply_text("Введите неотрицательное число.")
                return
        except ValueError:
            await update.message.reply_text("Введите число (например: 10 или 15000).")
            return
        year, month = state.get("year"), state.get("month")
        if not year or not month:
            year, month = today_moscow().year, today_moscow().month
        await upsert_month_plan(direction, year, month, value)
        del USER_STATE[user_id]
        ym = f"{year}-{month:02d}"
        await update.message.reply_text(
            f"✅ План для *{direction}* на {ym} сохранён: {_format_amount(direction, value)}.",
            parse_mode="Markdown",
            reply_markup=get_main_menu_keyboard(user_id),
        )
        return

    # Режим ввода планов на месяц пошагово (только РТТ)
    if state.get("mode") == "plan_new_month":
        if not is_rtt(user_id):
            del USER_STATE[user_id]
            return
        if state.get("step") == "month":
            try:
                # Допускаем 2026-03 или 03-2026
                parts = text.strip().replace(".", "-").replace("/", "-").split("-")
                parts = [p.strip() for p in parts if p.strip()]
                if len(parts) != 2:
                    raise ValueError
                a, b = int(parts[0]), int(parts[1])
                if a > 12 and b <= 12:
                    year, month = a, b
                elif a <= 12 and b > 12:
                    year, month = b, a
                elif a > 12 and b > 12:
                    raise ValueError
                else:
                    year, month = max(a, b), min(a, b)
                    if year < 100:
                        year += 2000
                _ = date(year, month, 1)
            except Exception:
                cur = today_moscow()
                await update.message.reply_text(
                    f"Неверный формат. Введите месяц: *Год-Месяц* (например {cur.year}-{cur.month:02d})."
                )
                return
            state["step"] = "values"
            state["year"] = year
            state["month"] = month
            state["index"] = 0
            state["values"] = {}
            directions = SALES_DIRECTIONS
            next_direction = directions[0]
            unit = DIRECTION_UNITS.get(next_direction, "₽")
            await update.message.reply_text(
                f"→ {next_direction} ({unit}):",
                reply_markup=get_plan_skip_cancel_keyboard(),
            )
            return
        if state.get("step") == "values":
            directions = SALES_DIRECTIONS
            index = state["index"]
            current_direction = directions[index]
            try:
                value = float(text.replace(",", "."))
                if value < 0:
                    await update.message.reply_text("Введите неотрицательное число или нажмите Skip.")
                    return
            except ValueError:
                await update.message.reply_text("Введите число или нажмите Skip (0).")
                return
            state["values"][current_direction] = value
            index += 1
            state["index"] = index
            if index >= len(directions):
                for d, val in state["values"].items():
                    await upsert_month_plan(d, state["year"], state["month"], val)
                del USER_STATE[user_id]
                ym = f"{state['year']}-{state['month']:02d}"
                await update.message.reply_text(
                    f"✅ Планы на {ym} сохранены.",
                    reply_markup=get_main_menu_keyboard(user_id),
                )
                return
            next_direction = directions[index]
            unit = DIRECTION_UNITS.get(next_direction, "₽")
            await update.message.reply_text(
                f"→ {next_direction} ({unit}):",
                reply_markup=get_plan_skip_cancel_keyboard(),
            )
            return

    # Режим корректировки сводки (только РТТ)
    if state.get("mode") == "correction":
        if not is_rtt(user_id):
            del USER_STATE[user_id]
            await update.message.reply_text("⛔ Недостаточно прав. Корректировать сводку может только РТТ.")
            return
        direction = state.get("direction", "")
        try:
            value = float(text.replace(",", "."))
            if value < 0:
                await update.message.reply_text("Введите неотрицательное число.")
                return
        except ValueError:
            await update.message.reply_text("Введите число (например: 10 или 15000).")
            return
        today = today_moscow()
        await set_daily_correction(today, direction, value)
        del USER_STATE[user_id]
        report_text = await build_summary_text(
            today_moscow(),
            title="📊 *Сводка по продажам* (с корректировками)",
            variant="day_chat",
        )
        sent = False
        for cid in REPORT_CHAT_IDS:
            try:
                await context.bot.send_message(cid, report_text, parse_mode="Markdown")
                sent = True
            except Exception as e:
                logger.warning("Не удалось отправить отчёт в чат %s: %s", cid, e)
        if sent:
            await update.message.reply_text(
                "✅ Корректировка сохранена. Рабочий отчёт отправлен в чат.",
                reply_markup=get_main_menu_keyboard(user_id),
            )
        else:
            await update.message.reply_text(
                "✅ Корректировка сохранена."
                + (" Настройте REPORT_CHAT_IDS в .env для отправки в чат." if not REPORT_CHAT_IDS else ""),
                reply_markup=get_main_menu_keyboard(user_id),
            )
        return

    # Режим «вечерний отчёт» — поочерёдный опрос по показателям
    if state.get("mode") == "evening":
        directions = state["directions"]
        index = state["index"]

        if text.lower() == "/cancel":
            del USER_STATE[user_id]
            await update.message.reply_text("Вечерний отчёт отменён.", reply_markup=get_main_menu_keyboard(user_id))
            return

        current_direction = directions[index]

        if text != "/skip":
            try:
                value = float(text.replace(",", "."))
                if value < 0:
                    await update.message.reply_text("Число ≥ 0 или /skip")
                    return
                state["values"][current_direction] = value
            except ValueError:
                await update.message.reply_text("Число или /skip")
                return

        # Переходим к следующему показателю
        index += 1
        state["index"] = index

        if index >= len(directions):
            # Записываем все введённые показатели как отчёты
            username = update.effective_user.username if update.effective_user else None
            total_reports = 0
            for direction, amount in state["values"].items():
                await add_report(
                    direction=direction,
                    amount=amount,
                    comment="Вечерний отчёт",
                    user_id=user_id,
                    username=username,
                )
                total_reports += 1
            today = today_moscow()
            USER_STATE[user_id] = {"mode": "report_comment", "type": "evening", "date": today}
            await update.message.reply_text(
                f"✅ Вечерний отчёт заполнен. Записано показателей: {total_reports}.\n\nДобавить комментарий к отчёту? (или /skip)",
            )
            return

        # Спрашиваем следующий показатель с кнопками Skip / Отмена
        next_direction = directions[index]
        unit = DIRECTION_UNITS.get(next_direction, "₽")
        await update.message.reply_text(
            f"→ {next_direction} ({unit}):",
            reply_markup=get_evening_skip_cancel_keyboard(),
        )
        return

    # Режим «дневной отчёт» — как вечерний, но после заполнения сводка уходит в чат
    if state.get("mode") == "daily":
        directions = state["directions"]
        index = state["index"]

        if text.lower() == "/cancel":
            del USER_STATE[user_id]
            await update.message.reply_text("Дневной отчёт отменён.", reply_markup=get_main_menu_keyboard(user_id))
            return

        current_direction = directions[index]

        if text != "/skip":
            try:
                value = float(text.replace(",", "."))
                if value < 0:
                    await update.message.reply_text("Число ≥ 0 или /skip")
                    return
                state["values"][current_direction] = value
            except ValueError:
                await update.message.reply_text("Число или /skip")
                return

        index += 1
        state["index"] = index

        if index >= len(directions):
            username = update.effective_user.username if update.effective_user else None
            for direction, amount in state["values"].items():
                await add_report(
                    direction=direction,
                    amount=amount,
                    comment="Дневной отчёт",
                    user_id=user_id,
                    username=username,
                )
            today = today_moscow()
            USER_STATE[user_id] = {"mode": "report_comment", "type": "daily", "date": today}
            await update.message.reply_text(
                "✅ Дневной отчёт заполнен.\n\nДобавить комментарий к отчёту? (или /skip)",
            )
            return

        next_direction = directions[index]
        unit = DIRECTION_UNITS.get(next_direction, "₽")
        await update.message.reply_text(
            f"→ {next_direction} ({unit}):",
            reply_markup=get_daily_skip_cancel_keyboard(),
        )
        return

    # Шаг комментария к отчёту (после заполнения дневного/вечернего)
    if state.get("mode") == "report_comment":
        report_type = state.get("type", "")
        report_date = state.get("date") or today_moscow()
        comment = text.strip() if text.strip().lower() != "/skip" else ""
        if comment:
            await set_report_comment(report_date, report_type, comment)
        USER_STATE[user_id] = {"mode": "confirm_send", "type": report_type, "date": report_date}
        await update.message.reply_text(
            "Отправить сводку в чат?",
            reply_markup=get_confirm_send_keyboard(report_type),
        )
        return

    # Обычный режим добавления одиночного отчёта
    if state["step"] == "amount":
        try:
            amount = float(text.replace(",", "."))
            if amount < 0:
                await update.message.reply_text("Сумма должна быть положительной.")
                return
            state["amount"] = amount
            state["step"] = "comment"
            await update.message.reply_text(
                "Введите комментарий (или /skip чтобы пропустить):"
            )
        except ValueError:
            await update.message.reply_text("Введите число, например: 15000 или 15.5")

    elif state["step"] == "comment":
        if text == "/skip":
            comment = ""
        else:
            comment = text
        direction = state["direction"]
        amount = state["amount"]
        username = update.effective_user.username if update.effective_user else None

        report_id = await add_report(
            direction=direction,
            amount=amount,
            comment=comment,
            user_id=user_id,
            username=username,
        )
        del USER_STATE[user_id]
        formatted_amount = _format_amount(direction, amount)
        await update.message.reply_text(
            f"✅ Отчёт #{report_id} добавлен!\n"
            f"📌 {direction}: {formatted_amount}"
            + (f"\n💬 {comment}" if comment else ""),
            reply_markup=get_main_menu_keyboard(user_id),
        )


async def build_summary_text(
    target_date: date,
    title: str | None = None,
    variant: str = "day",
) -> str:
    """
    Собрать текст сводки.
    variant "day" — отчёты в течение дня: Осталось | План/день | Выполнено.
    variant "evening" — вечерний финальный: План на месяц | Выполнено за месяц | Сегодня.
    """
    daily = await get_daily_summary(target_date)
    monthly = await get_month_summary(target_date)
    plans = await get_month_plans(target_date)

    if not daily and not monthly and not plans:
        return "📊 За сегодня и за месяц отчётов пока нет."

    daily_by_dir = {row["direction"]: dict(row) for row in daily}
    month_by_dir = {row["direction"]: dict(row) for row in monthly}

    # Применяем корректировки за день
    corrections = await get_daily_corrections(target_date)
    day_orig = {d: daily_by_dir[d]["total"] for d in daily_by_dir}
    for d, val in corrections.items():
        if d in daily_by_dir:
            daily_by_dir[d]["total"] = val
        else:
            daily_by_dir[d] = {"total": val, "count": 0}
    for d in list(month_by_dir.keys()):
        day_old = day_orig.get(d, 0)
        day_new = daily_by_dir.get(d, {}).get("total", 0)
        month_by_dir[d]["total"] = month_by_dir[d]["total"] - day_old + day_new
    for d in corrections:
        if d not in month_by_dir:
            month_by_dir[d] = {"total": corrections[d], "count": 0}

    all_dirs = list(
        dict.fromkeys(
            list(SALES_DIRECTIONS)
            + list(daily_by_dir.keys())
            + list(month_by_dir.keys())
            + list(plans.keys())
        )
    )

    total_today_money = sum(
        daily_by_dir[d]["total"] for d in daily_by_dir if not _is_piece_direction(d)
    )
    total_month_money = sum(
        month_by_dir[d]["total"] for d in month_by_dir if not _is_piece_direction(d)
    )
    total_plan_money = sum(
        plan
        for direction, plan in plans.items()
        if not _is_piece_direction(direction)
    ) if plans else 0.0

    if title is None:
        title = "📊 *Сводка по продажам*"

    days_in_month = calendar.monthrange(target_date.year, target_date.month)[1]
    # Оставшиеся дни в месяце (включая сегодня) — для расчёта плана на день
    remaining_days_in_month = max(1, days_in_month - target_date.day + 1)
    printed_dirs = set()

    if variant == "evening":
        # Вечерний финальный: План на месяц | Выполнено за месяц | Сегодня
        header = "План на месяц | Выполнено за месяц | Сегодня"
    elif variant == "day_chat":
        # В чат — кратко: План/день | Выполнено за день (сокращения)
        header = "План/день | Выполнено за день"
    else:
        # В боте по кнопке Сводка — полная: все показатели
        header = "План на месяц | Осталось | План/день | Выполнено"

    lines = [
        title,
        f"Дата: {target_date.strftime('%d.%m.%Y')}",
        "",
        header,
        "",
    ]

    def _append_direction_block(direction: str) -> bool:
        d_day = daily_by_dir.get(direction)
        d_month = month_by_dir.get(direction)
        plan = plans.get(direction, 0.0)
        if plan is not None:
            plan = float(plan)
        else:
            plan = 0.0
        month_total = d_month["total"] if d_month else 0.0
        day_total = d_day["total"] if d_day else 0.0

        if not (month_total or day_total or plan):
            return False

        if variant == "evening":
            plan_str = _format_amount(direction, plan) if plan > 0 else "—"
            month_str = _format_amount(direction, month_total)
            today_str = _format_amount(direction, day_total)
            # Индикатор: ✅ план за день выполнен, ❌ не выполнен
            if plan > 0 and days_in_month > 0:
                plan_per_day = plan / days_in_month
                indicator = "✅" if day_total >= plan_per_day else "❌"
            else:
                indicator = "➖"
            line = f"{indicator} *{direction}*: {plan_str} | {month_str} | {today_str}"
        elif variant == "day_chat":
            # В чат — кратко: сокращения, только План/день | Выполнено за день
            if plan > 0:
                remaining = plan - month_total
                if remaining > 0 and remaining_days_in_month > 0:
                    plan_per_day = remaining / remaining_days_in_month
                    plan_day_str = _format_amount(direction, plan_per_day)
                else:
                    plan_day_str = _format_amount(direction, 0)
            else:
                plan_day_str = "—"
            completed_day_str = _format_amount(direction, day_total)
            short = DIRECTION_SHORT.get(direction, direction)
            line = f"*{short}*: {plan_day_str} | {completed_day_str}"
        else:
            # В боте (кнопка Сводка) — полная: План на месяц | Осталось | План/день | Выполнено
            if plan > 0:
                plan_str = _format_amount(direction, plan)
                remaining = plan - month_total
                remaining_str = _format_amount(direction, remaining)
                if remaining > 0 and remaining_days_in_month > 0:
                    plan_per_day = remaining / remaining_days_in_month
                    plan_day_str = _format_amount(direction, plan_per_day)
                else:
                    plan_day_str = _format_amount(direction, 0)
            else:
                plan_str = "—"
                remaining_str = "—"
                plan_day_str = "—"
            completed_str = _format_amount(direction, month_total)
            line = f"*{direction}*: {plan_str} | {remaining_str} | {plan_day_str} | {completed_str}"

        lines.append(line)
        printed_dirs.add(direction)
        return True

    for block in DIRECTION_BLOCKS:
        block_has_any = False
        for direction in block:
            if direction not in all_dirs:
                continue
            if _append_direction_block(direction):
                block_has_any = True
        if block_has_any:
            lines.append("")

    for direction in all_dirs:
        if direction in printed_dirs:
            continue
        if _append_direction_block(direction):
            lines.append("")

    today_int = math.ceil(total_today_money) if total_today_money != int(total_today_money) else int(total_today_money)
    lines.append(
        f"💰 *Итого за сегодня (в сумме):* {today_int:,} ₽"
    )
    if total_plan_money > 0:
        total_progress = (total_month_money / total_plan_money) * 100
        month_int = math.ceil(total_month_money) if total_month_money != int(total_month_money) else int(total_month_money)
        plan_int = math.ceil(total_plan_money) if total_plan_money != int(total_plan_money) else int(total_plan_money)
        lines.append(
            f"📅 *Итого за месяц (в сумме):* "
            f"{month_int:,} ₽ из {plan_int:,} ₽ "
            f"({total_progress:.1f}% плана)"
        )
    else:
        month_int = math.ceil(total_month_money) if total_month_money != int(total_month_money) else int(total_month_money)
        lines.append(
            f"📅 *Итого за месяц (в сумме):* {month_int:,} ₽"
        )

    comment_type = "daily" if variant in ("day", "day_chat") else "evening" if variant == "evening" else None
    if comment_type:
        comment = await get_report_comment(target_date, comment_type)
        if comment:
            lines.append("")
            lines.append(f"💬 *Комментарий:* {comment}")

    return "\n".join(lines)


def _parse_date_arg(arg: str):
    """Парсит дату из аргумента: DD.MM, DD.MM.YYYY или YYYY-MM-DD. Возвращает date или None."""
    arg = arg.strip().replace("-", ".").replace("/", ".")
    parts = [p for p in arg.split(".") if p]
    try:
        if len(parts) == 2:  # DD.MM
            d, m = int(parts[0]), int(parts[1])
            y = today_moscow().year
            return date(y, m, d)
        if len(parts) == 3:
            a, b, c = int(parts[0]), int(parts[1]), int(parts[2])
            if c > 31:  # YYYY.MM.DD
                return date(a, b, c)
            if a > 31:  # YYYY.MM.DD
                return date(a, b, c)
            return date(c, b, a)  # DD.MM.YYYY
    except (ValueError, TypeError):
        pass
    return None


async def cmd_summary(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Сводка: день + месяц/план. /summary или /summary DD.MM — за другую дату. Кнопка «Корректировать» только у РТТ."""
    if not await check_access(update, context):
        return
    today = today_moscow()
    target_date = today
    if context.args:
        parsed = _parse_date_arg(context.args[0])
        if parsed:
            target_date = parsed
        else:
            await update.message.reply_text("Неверный формат даты. Используйте: DD.MM или DD.MM.YYYY (например 02.03)")
            return
    text = await build_summary_text(target_date)
    user_id = update.effective_user.id if update.effective_user else 0
    reply_markup = get_summary_correct_keyboard() if is_rtt(user_id) and target_date == today else None
    await update.message.reply_text(
        text,
        parse_mode="Markdown",
        reply_markup=reply_markup,
    )


async def cmd_today_quick(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Краткая строка: как дела сегодня — % плана и сколько направлений в норме."""
    if not await check_access(update, context):
        return
    today = today_moscow()
    daily = await get_daily_summary(today)
    monthly = await get_month_summary(today)
    plans = await get_month_plans(today)
    daily_by_dir = {row["direction"]: dict(row) for row in daily}
    month_by_dir = {row["direction"]: dict(row) for row in monthly}
    total_plan_money = sum(
        p for d, p in plans.items() if not _is_piece_direction(d)
    ) if plans else 0.0
    total_month_money = sum(
        month_by_dir[d]["total"] for d in month_by_dir if not _is_piece_direction(d)
    )
    days_in_month = calendar.monthrange(today.year, today.month)[1]
    plan_per_day_by_dir = {}
    if days_in_month > 0:
        for d, p in plans.items():
            if p and p > 0:
                plan_per_day_by_dir[d] = p / days_in_month
    on_track = 0
    total_with_plan = 0
    for direction, plan_per_day in plan_per_day_by_dir.items():
        day_val = daily_by_dir.get(direction, {}).get("total") or 0
        total_with_plan += 1
        if day_val >= plan_per_day:
            on_track += 1
    if total_plan_money > 0:
        pct = (total_month_money / total_plan_money) * 100
        line = f"📌 *Сегодня* ({today.strftime('%d.%m.%Y')}): *{pct:.1f}%* плана по сумме"
    else:
        line = f"📌 *Сегодня* ({today.strftime('%d.%m.%Y')}): планы не заданы"
    if total_with_plan > 0:
        line += f", {on_track} из {total_with_plan} направлений в норме по плану/день."
    else:
        line += "."
    await update.message.reply_text(line, parse_mode="Markdown")


async def cmd_plans(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Показать планы на месяц. У РТТ — кнопки «Корректировать» и «Внести планы на месяц»."""
    if not await check_access(update, context):
        return

    user_id = update.effective_user.id if update.effective_user else 0
    target_date = today_moscow()
    if context.args:
        month_str = context.args[0].strip().replace(".", "-").replace("/", "-")
        try:
            parts = month_str.split("-")
            if len(parts) != 2:
                raise ValueError
            a, b = int(parts[0]), int(parts[1])
            if a > 12 and b <= 12:
                year, month = a, b
            elif a <= 12 and b > 12:
                year, month = b, a
            else:
                year, month = max(a, b), min(a, b)
                if year < 100:
                    year += 2000
            target_date = date(year, month, 1)
        except Exception:
            await update.message.reply_text(
                "Неверный формат месяца. Используйте: YYYY-MM или MM-YYYY (например 2026-03)"
            )
            return

    plans = await get_month_plans(target_date)
    ym = target_date.strftime("%Y-%m")

    if not plans:
        text = (
            f"📈 *Планы на месяц {ym}*\n\n"
            "Планы на этот месяц ещё не заданы.\n\n"
            "РТТ: нажмите «➕ Внести планы на месяц» и введите месяц в формате *Год-Месяц* (например " + ym + ")."
        )
    else:
        lines = [f"📈 *Планы на месяц {ym}*"]
        for block in DIRECTION_BLOCKS:
            block_has_any = False
            for direction in block:
                plan = plans.get(direction)
                if plan is None:
                    continue
                lines.append(f"• {direction}: {_format_amount(direction, plan)}")
                block_has_any = True
            if block_has_any:
                lines.append("")
        text = "\n".join(lines)

    reply_markup = get_plans_actions_keyboard() if is_rtt(user_id) else None
    await update.message.reply_text(
        text,
        parse_mode="Markdown",
        reply_markup=reply_markup,
    )


async def cmd_setplan(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Задать или изменить месячный план. Только РТТ."""
    if not await check_access(update, context):
        return
    user_id = update.effective_user.id if update.effective_user else 0
    if not is_rtt(user_id):
        await update.message.reply_text(
            "⛔ Недостаточно прав. Корректировать планы на месяц может только РТТ."
        )
        return

    args = context.args
    if len(args) < 3:
        await update.message.reply_text(
            "Использование:\n"
            "/setplan YYYY-MM <направление> <сумма>\n"
            "Пример: /setplan 2025-03 Розница 1500000"
        )
        return

    month_str = args[0]
    try:
        year, month = map(int, month_str.split("-"))
        _ = date(year, month, 1)
    except Exception:
        await update.message.reply_text(
            "Неверный формат месяца. Используйте: YYYY-MM, например 2025-03"
        )
        return

    direction = " ".join(args[1:-1])
    if direction not in SALES_DIRECTIONS:
        await update.message.reply_text(
            "Неизвестное направление.\n"
            f"Доступные: {', '.join(SALES_DIRECTIONS)}"
        )
        return

    amount_str = args[-1]
    try:
        plan_amount = float(amount_str.replace(",", "."))
        if plan_amount <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text(
            "Сумма плана должна быть положительным числом.\n"
            "Пример: 1500000 или 1500.50"
        )
        return

    await upsert_month_plan(direction, year, month, plan_amount)

    plan_display = math.ceil(plan_amount) if plan_amount != int(plan_amount) else int(plan_amount)
    await update.message.reply_text(
        f"✅ План на {month_str} для направления *{direction}* "
        f"установлен: {plan_display:,} ₽",
        parse_mode="Markdown",
    )


def _parse_ym_arg(args: list) -> tuple[int, int] | None:
    """Парсит YYYY-MM из args; если пусто — текущий месяц по Москве."""
    today = today_moscow()
    if not args:
        return today.year, today.month
    s = args[0].strip().replace(".", "-").replace("/", "-")
    parts = s.split("-")
    if len(parts) != 2:
        return None
    try:
        a, b = int(parts[0]), int(parts[1])
        if a > 12 and b <= 12:
            return a, b
        if a <= 12 and b > 12:
            return b, a
        y, m = max(a, b), min(a, b)
        if y < 100:
            y += 2000
        return y, m
    except (ValueError, TypeError):
        return None


async def cmd_export(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Экспорт планов или отчётов в CSV. Только РТТ. /export plans [YYYY-MM] или /export reports [YYYY-MM]."""
    if not await check_access(update, context):
        return
    user_id = update.effective_user.id if update.effective_user else 0
    if not is_rtt(user_id):
        await update.message.reply_text("⛔ Экспорт доступен только РТТ.")
        return
    args = (context.args or [])
    if not args:
        await update.message.reply_text(
            "Использование:\n/export plans [YYYY-MM]\n/export reports [YYYY-MM]\n"
            "Месяц по умолчанию — текущий."
        )
        return
    kind = args[0].lower()
    ym = _parse_ym_arg(args[1:]) if len(args) > 1 else _parse_ym_arg([])
    if not ym:
        await update.message.reply_text("Неверный формат месяца. Используйте YYYY-MM.")
        return
    year, month = ym
    buf = io.StringIO()
    writer = csv.writer(buf)
    if kind == "plans":
        rows = await get_plans_export(year, month)
        writer.writerow(["direction", "year", "month", "plan"])
        for r in rows:
            writer.writerow([r.get("direction", ""), r.get("year"), r.get("month"), r.get("plan", 0)])
        filename = f"plans_{year}-{month:02d}.csv"
    elif kind == "reports":
        rows = await get_reports_export(year, month)
        writer.writerow(["report_date", "direction", "amount", "comment", "report_time"])
        for r in rows:
            writer.writerow([
                r.get("report_date", ""), r.get("direction", ""), r.get("amount", 0),
                r.get("comment", ""), r.get("report_time", ""),
            ])
        filename = f"reports_{year}-{month:02d}.csv"
    else:
        await update.message.reply_text("Укажите: plans или reports.")
        return
    buf.seek(0)
    csv_bytes = buf.getvalue().encode("utf-8-sig")
    await update.message.reply_document(
        document=InputFile(io.BytesIO(csv_bytes), filename=filename),
        caption=f"📎 {filename}",
    )


def format_focus_report(report_time: str, summary: list, period_total_money: float) -> str:
    """Форматирование фокусного отчёта."""
    lines = [f"📌 *Фокусный отчёт на {report_time}*\n"]
    if not summary:
        lines.append("За последние 2 часа отчётов нет.")
    else:
        for r in summary:
            comment = f" — {r['comment']}" if r.get("comment") else ""
            lines.append(
                f"• {r['direction']}: {_format_amount(r['direction'], r['amount'])}{comment} ({r.get('username', '?')})"
            )
        period_int = math.ceil(period_total_money) if period_total_money != int(period_total_money) else int(period_total_money)
        lines.append(f"\n💰 За период (в сумме): {period_int:,} ₽")
    return "\n".join(lines)


async def send_daily_pre_reminder(context: ContextTypes.DEFAULT_TYPE):
    """За 10 минут до дневного отчёта (11:50, 15:50, 18:50): уведомление в чат."""
    job = context.job
    report_hour = job.data["time"]
    time_str = f"{report_hour:02d}:00"

    text = (
        f"⏰ *Через 10 минут* — дневной отчёт на {time_str}.\n\n"
        "Зайдите в бота (ЛС): кнопка «📋 Дневной отчёт» или /daily.\n"
        "После заполнения сводка придёт сюда в чат."
    )

    for chat_id in REPORT_CHAT_IDS:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode="Markdown",
            )
            logger.info(f"Sent daily pre-reminder (before {time_str}) to {chat_id}")
        except Exception as e:
            logger.error(f"Failed to send daily pre-reminder to {chat_id}: {e}")

    if not REPORT_CHAT_IDS:
        logger.info("Daily pre-reminder (no REPORT_CHAT_IDS): before %s", time_str)


async def send_scheduled_report(context: ContextTypes.DEFAULT_TYPE):
    """По расписанию (12, 16, 19): только напоминание. Сводка уйдёт в чат после того, как сотрудник заполнит дневной отчёт в боте."""
    job = context.job
    report_time = job.data["time"]
    time_str = f"{report_time:02d}:00"

    text = (
        f"⏰ *Дневной отчёт на {time_str}*\n\n"
        "Зайдите в бота в *личные сообщения* и заполните показатели:\n"
        "кнопка «📋 Дневной отчёт» или команда /daily.\n\n"
        "После заполнения сводка придёт сюда в чат."
    )

    for chat_id in REPORT_CHAT_IDS:
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text=text,
                parse_mode="Markdown",
            )
            logger.info(f"Sent daily reminder to {chat_id}")
        except Exception as e:
            logger.error(f"Failed to send reminder to {chat_id}: {e}")

    if not REPORT_CHAT_IDS:
        logger.info("Daily reminder (no REPORT_CHAT_IDS): %s", time_str)


async def _send_evening_reminder_message(context, text: str) -> None:
    """Разослать текст напоминания по чатам с вечерним уведомлением."""
    chat_ids = await get_evening_chats()
    for chat_id in chat_ids:
        try:
            await context.bot.send_message(chat_id, text, parse_mode="Markdown")
            logger.info(f"Sent evening reminder to {chat_id}")
        except Exception as e:
            logger.error(f"Failed to send evening reminder to {chat_id}: {e}")
    if not chat_ids:
        logger.info("Evening reminder: no subscribed chats")


async def send_evening_reminder(context: ContextTypes.DEFAULT_TYPE):
    """Напоминание в 20:50 — только если сводка ещё не отправлена. Если не начинали — «заполните»; если начали, но не закончили — «дозаполните до конца»."""
    today = today_moscow()
    if await was_evening_summary_sent(today):
        return
    count = await count_evening_reports_today(today)
    if count == 0:
        text = (
            "⏰ *Напоминание: вечерний отчёт*\n\n"
            "Через 10 минут в 21:00 — время отчитаться.\n"
            "Зайдите в бота (ЛС): кнопка «🌙 Вечерний отчёт» или /evening.\n\n"
            "После заполнения сводка придёт сюда в чат."
        )
    else:
        text = (
            "⏰ *Напоминание: вечерний отчёт*\n\n"
            "Вы начали заполнять вечерний отчёт, но не завершили.\n"
            "Зайдите в бота (ЛС) и заполните показатели до конца — тогда сводка придёт в чат."
        )
    await _send_evening_reminder_message(context, text)


async def send_evening_repeat_reminder(context: ContextTypes.DEFAULT_TYPE):
    """Повторное напоминание в 21:00 — только если сводка ещё не отправлена. Тот же выбор: не начинали / начали, но не закончили."""
    today = today_moscow()
    if await was_evening_summary_sent(today):
        return
    count = await count_evening_reports_today(today)
    if count == 0:
        text = (
            "⏰ *Напоминание: вечерний отчёт*\n\n"
            "Заполните показатели в боте (ЛС): кнопка «🌙 Вечерний отчёт» или /evening.\n\n"
            "После заполнения сводка придёт сюда в чат."
        )
    else:
        text = (
            "⏰ *Напоминание: вечерний отчёт*\n\n"
            "Вы начали заполнять отчёт, но не завершили. Зайдите в бота (ЛС) и заполните до конца — сводка придёт в чат."
        )
    await _send_evening_reminder_message(context, text)


def main():
    """Запуск бота."""
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN не задан. Укажите его в .env и перезапустите бота.")
        return

    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", cmd_start))
    application.add_handler(CommandHandler("help", cmd_help))
    application.add_handler(CommandHandler("report", cmd_report))
    application.add_handler(CommandHandler("summary", cmd_summary))
    application.add_handler(CommandHandler("daily", cmd_daily))
    application.add_handler(CommandHandler("evening", cmd_evening))
    application.add_handler(CommandHandler("plans", cmd_plans))
    application.add_handler(CommandHandler("setplan", cmd_setplan))
    application.add_handler(CommandHandler("export", cmd_export))
    application.add_handler(CommandHandler("pending", cmd_pending))
    application.add_handler(CommandHandler("notify", cmd_notify))
    application.add_handler(CallbackQueryHandler(callback_access_confirm_reject, pattern="^access_(confirm|reject)_"))
    application.add_handler(CallbackQueryHandler(callback_plan_actions, pattern="^plan_"))
    application.add_handler(CallbackQueryHandler(callback_confirm_send, pattern="^send_confirm_"))
    application.add_handler(CallbackQueryHandler(callback_notifications, pattern="^notif_"))
    application.add_handler(CallbackQueryHandler(callback_summary_correct, pattern="^(summary_corr|corr_)"))
    application.add_handler(CallbackQueryHandler(callback_daily_buttons, pattern="^daily_"))
    application.add_handler(CallbackQueryHandler(callback_evening_buttons, pattern="^ev_"))
    application.add_handler(CallbackQueryHandler(callback_direction, pattern="^dir_"))
    application.add_handler(
        MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message)
    )

    async def post_init(app: Application):
        await init_db()
        if app.job_queue:
            tz = ZoneInfo("Europe/Moscow")
            for hour in REPORT_SCHEDULE:
                # За 10 минут до отчёта — уведомление (11:50, 15:50, 18:50)
                app.job_queue.run_daily(
                    send_daily_pre_reminder,
                    time=dt_time(hour - 1, 50, tzinfo=tz),
                    name=f"daily_pre_{hour}",
                    data={"time": hour},
                )
                # В момент отчёта — напоминание заполнить (12:00, 16:00, 19:00)
                app.job_queue.run_daily(
                    send_scheduled_report,
                    time=dt_time(hour, 0, tzinfo=tz),
                    name=f"report_{hour}",
                    data={"time": hour},
                )
            # Напоминание о вечернем отчёте в 20:50 (только если сводка ещё не отправлена)
            app.job_queue.run_daily(
                send_evening_reminder,
                time=dt_time(20, 50, tzinfo=tz),
                name="evening_reminder",
            )
            # В 21:00 — повторное напоминание заполнить вечерний отчёт (сводка уходит в чат только после заполнения сотрудником)
            app.job_queue.run_daily(
                send_evening_repeat_reminder,
                time=dt_time(21, 0, tzinfo=tz),
                name="evening_repeat_reminder",
            )
            logger.info(f"Scheduled: daily pre-reminders 10 min before {REPORT_SCHEDULE}, reports at {REPORT_SCHEDULE}, evening 20:50 & 21:00")
        else:
            logger.warning("Job queue not available - install python-telegram-bot[job-queue]")

    application.post_init = post_init

    application.run_polling(allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()
